//! The relay's sync read endpoints, as a `Router` fragment: listRepos,
//! getRepoStatus, getLatestCommit, listHosts and getHostStatus. Field and
//! error names follow the lexicons in vlpds/lexicons.

use crate::state::{AccountStatus, Chain, HostPage, HostRecord, HostStore, Record, RepoPage, StateStore, StoreError};
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use vlpds::xrpc::XrpcError;

/// What the handlers read. `StateStore` implements it for the shards this
/// node owns; a cluster wrapper can route to the owner instead.
#[async_trait::async_trait]
pub trait SyncSource: Send + Sync {
    async fn list_repos(&self, cursor: Option<&str>, limit: usize) -> Result<RepoPage, StoreError>;
    async fn repo(&self, did: &str) -> Result<Option<Arc<Record>>, StoreError>;
    async fn host(&self, hostname: &str) -> anyhow::Result<Option<HostRecord>>;
    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<HostPage>;
}

#[async_trait::async_trait]
impl<C: Chain> SyncSource for StateStore<C> {
    async fn list_repos(&self, cursor: Option<&str>, limit: usize) -> Result<RepoPage, StoreError> {
        StateStore::list_repos(self, cursor, limit).await
    }
    async fn repo(&self, did: &str) -> Result<Option<Arc<Record>>, StoreError> {
        self.get(did).await
    }
    async fn host(&self, hostname: &str) -> anyhow::Result<Option<HostRecord>> {
        self.get_host(hostname).await
    }
    async fn list_hosts(&self, cursor: Option<&str>, limit: usize) -> anyhow::Result<HostPage> {
        HostStore::list_hosts(self, cursor, limit).await
    }
}

type Src = Arc<dyn SyncSource>;
type Params = Query<HashMap<String, String>>;
type XResult = Result<Json<Value>, XrpcError>;

pub fn router(src: Src) -> Router {
    Router::new()
        .route("/xrpc/com.atproto.sync.listRepos", get(list_repos))
        .route("/xrpc/com.atproto.sync.getRepoStatus", get(get_repo_status))
        .route("/xrpc/com.atproto.sync.getLatestCommit", get(get_latest_commit))
        .route("/xrpc/com.atproto.sync.listHosts", get(list_hosts))
        .route("/xrpc/com.atproto.sync.getHostStatus", get(get_host_status))
        .with_state(src)
}

fn store_err(e: StoreError) -> XrpcError {
    match e {
        StoreError::NotOwner(s) => XrpcError::unavailable("ShardUnavailable", format!("shard {s} is not served here")),
        StoreError::BadCursor => XrpcError::bad("InvalidRequest", "bad cursor"),
        e => XrpcError::internal(e.to_string()),
    }
}

fn any_err(e: anyhow::Error) -> XrpcError {
    match e.downcast::<StoreError>() {
        Ok(e) => store_err(e),
        Err(e) => XrpcError::internal(e.to_string()),
    }
}

fn limit(p: &HashMap<String, String>, default: usize) -> Result<usize, XrpcError> {
    match p.get("limit") {
        None => Ok(default),
        Some(s) => match s.parse::<usize>() {
            Ok(n) if (1..=1000).contains(&n) => Ok(n),
            _ => Err(XrpcError::bad("InvalidRequest", "limit must be an integer from 1 to 1000")),
        },
    }
}

fn did_param(p: &HashMap<String, String>) -> Result<&str, XrpcError> {
    let did = p.get("did").ok_or_else(|| XrpcError::bad("InvalidRequest", "missing did"))?;
    let ok = did.len() <= 2048
        && did.strip_prefix("did:").and_then(|r| r.split_once(':')).is_some_and(|(m, id)| {
            !m.is_empty() && m.bytes().all(|b| b.is_ascii_lowercase()) && !id.is_empty() && !id.ends_with(':')
        });
    if !ok {
        return Err(XrpcError::bad("InvalidRequest", "did is not a valid DID"));
    }
    Ok(did)
}

fn cursor(p: &HashMap<String, String>) -> Option<&str> {
    p.get("cursor").map(String::as_str).filter(|c| !c.is_empty())
}

async fn list_repos(State(src): State<Src>, Query(p): Params) -> XResult {
    let page = src.list_repos(cursor(&p), limit(&p, 500)?).await.map_err(store_err)?;
    let repos: Vec<Value> = page
        .repos
        .iter()
        .map(|r| {
            let mut v = json!({
                "did": r.did,
                "head": r.head.to_string(),
                "rev": r.rev.to_string(),
                "active": r.status.is_active(),
            });
            if let Some(s) = r.status.as_str() {
                v["status"] = json!(s);
            }
            v
        })
        .collect();
    let mut out = json!({ "repos": repos });
    if let Some(c) = page.cursor {
        out["cursor"] = json!(c);
    }
    Ok(Json(out))
}

fn repo_not_found(did: &str) -> XrpcError {
    XrpcError::bad("RepoNotFound", format!("repo not found: {did}"))
}

async fn get_repo_status(State(src): State<Src>, Query(p): Params) -> XResult {
    let did = did_param(&p)?;
    let rec = src.repo(did).await.map_err(store_err)?.ok_or_else(|| repo_not_found(did))?;
    let st = rec.status();
    let mut out = json!({ "did": did, "active": st.is_active() });
    if let Some(s) = st.as_str() {
        out["status"] = json!(s);
    }
    if st.is_active()
        && let Some(c) = rec.chain
    {
        out["rev"] = json!(c.rev.to_string());
    }
    Ok(Json(out))
}

async fn get_latest_commit(State(src): State<Src>, Query(p): Params) -> XResult {
    let did = did_param(&p)?;
    let rec = src.repo(did).await.map_err(store_err)?.ok_or_else(|| repo_not_found(did))?;
    match rec.status() {
        AccountStatus::Takendown => return Err(XrpcError::bad("RepoTakendown", format!("repo has been taken down: {did}"))),
        AccountStatus::Suspended => return Err(XrpcError::bad("RepoSuspended", format!("repo is suspended: {did}"))),
        AccountStatus::Deactivated => {
            return Err(XrpcError::bad("RepoDeactivated", format!("repo has been deactivated: {did}")));
        }
        AccountStatus::Deleted | AccountStatus::Inactive => return Err(repo_not_found(did)),
        AccountStatus::Active | AccountStatus::Desynchronized | AccountStatus::Throttled => {}
    }
    let c = rec.chain.ok_or_else(|| repo_not_found(did))?;
    Ok(Json(json!({ "cid": c.commit.to_string(), "rev": c.rev.to_string() })))
}

fn host_json(h: &HostRecord) -> Value {
    json!({
        "hostname": h.hostname,
        "seq": h.cursor,
        "accountCount": h.account_count.max(0),
        "status": h.lexicon_status(),
    })
}

async fn list_hosts(State(src): State<Src>, Query(p): Params) -> XResult {
    let page = src.list_hosts(cursor(&p), limit(&p, 200)?).await.map_err(any_err)?;
    let mut out = json!({ "hosts": page.hosts.iter().map(host_json).collect::<Vec<_>>() });
    if let Some(c) = page.cursor {
        out["cursor"] = json!(c);
    }
    Ok(Json(out))
}

async fn get_host_status(State(src): State<Src>, Query(p): Params) -> XResult {
    let hostname = p
        .get("hostname")
        .map(|h| h.trim().to_ascii_lowercase())
        .filter(|h| !h.is_empty())
        .ok_or_else(|| XrpcError::bad("InvalidRequest", "missing hostname"))?;
    let h = src
        .host(&hostname)
        .await
        .map_err(any_err)?
        .ok_or_else(|| XrpcError::bad("HostNotFound", format!("host not found: {hostname}")))?;
    Ok(Json(host_json(&h)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::tests::{MapIdentity, claim, open, plc};
    use crate::state::{ApplyConfig, Applied, EventKind, HostRecord, Incoming, Tier};
    use crate::types::Host;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn call(app: &Router, uri: &str) -> (StatusCode, Value) {
        let r = app.clone().oneshot(Request::get(uri).body(Body::empty()).unwrap()).await.unwrap();
        let status = r.status();
        let b = axum::body::to_bytes(r.into_body(), 1 << 20).await.unwrap();
        (status, serde_json::from_slice(&b).unwrap())
    }

    #[tokio::test]
    async fn endpoints_match_the_lexicons() {
        let id = MapIdentity::new();
        let st = open(4, id.clone(), ApplyConfig::default()).await;
        let h = Host("pds.a".into());
        let mut tickets = Vec::new();
        let dids: Vec<String> = (0..5).map(|n| plc(500 + n)).collect();
        for d in &dids {
            id.set(d, "pds.a", 1);
            let Applied::Append(a) =
                st.apply(Incoming { did: d, host: &h, now: 100, kind: EventKind::Commit(claim(d, 1)) }).await.unwrap()
            else {
                panic!()
            };
            tickets.push(a.ticket);
        }
        for (d, status) in [(&dids[1], "takendown"), (&dids[2], "deactivated"), (&dids[3], "suspended")] {
            let Applied::Append(a) = st
                .apply(Incoming {
                    did: d,
                    host: &h,
                    now: 100,
                    kind: EventKind::Account { active: false, status: Some(status.into()) },
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            tickets.push(a.ticket);
        }
        st.commit(&tickets).await.unwrap();
        st.put_host(&HostRecord::new("pds.a", Tier::Default, 1)).await.unwrap();
        st.flush_host_counts(&*st).await.unwrap();
        st.checkpoint_cursors(&[("pds.a".into(), 77)]).await.unwrap();
        let app = router(st.clone());

        let (s, v) = call(&app, "/xrpc/com.atproto.sync.listRepos?limit=2").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(v["repos"].as_array().unwrap().len(), 2);
        let r0 = &v["repos"][0];
        for f in ["did", "head", "rev", "active"] {
            assert!(!r0[f].is_null(), "{f} in {r0}");
        }
        let c = v["cursor"].as_str().unwrap().to_string();
        let (_, v2) = call(&app, &format!("/xrpc/com.atproto.sync.listRepos?limit=1000&cursor={c}")).await;
        assert_eq!(v2["repos"].as_array().unwrap().len(), 3);
        assert!(v2.get("cursor").is_none());
        let all: Vec<Value> =
            v["repos"].as_array().unwrap().iter().chain(v2["repos"].as_array().unwrap()).cloned().collect();
        let td = all.iter().find(|r| r["did"] == dids[1].as_str()).unwrap();
        assert_eq!((td["active"].as_bool(), td["status"].as_str()), (Some(false), Some("takendown")));
        let ok = all.iter().find(|r| r["did"] == dids[0].as_str()).unwrap();
        assert!(ok.get("status").is_none());
        assert_eq!(ok["head"], claim(&dids[0], 1).commit.to_string());
        assert_eq!(call(&app, "/xrpc/com.atproto.sync.listRepos?limit=0").await.0, StatusCode::BAD_REQUEST);
        assert_eq!(call(&app, "/xrpc/com.atproto.sync.listRepos?limit=1001").await.0, StatusCode::BAD_REQUEST);

        let (s, v) = call(&app, &format!("/xrpc/com.atproto.sync.getRepoStatus?did={}", dids[0])).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(v, json!({"did": dids[0], "active": true, "rev": claim(&dids[0], 1).rev.to_string()}));
        let (_, v) = call(&app, &format!("/xrpc/com.atproto.sync.getRepoStatus?did={}", dids[2])).await;
        assert_eq!(v, json!({"did": dids[2], "active": false, "status": "deactivated"}));
        let (s, v) = call(&app, &format!("/xrpc/com.atproto.sync.getRepoStatus?did={}", plc(9999))).await;
        assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("RepoNotFound")));
        let (s, v) = call(&app, "/xrpc/com.atproto.sync.getRepoStatus?did=nope").await;
        assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("InvalidRequest")));
        assert_eq!(call(&app, "/xrpc/com.atproto.sync.getRepoStatus").await.0, StatusCode::BAD_REQUEST);

        let (s, v) = call(&app, &format!("/xrpc/com.atproto.sync.getLatestCommit?did={}", dids[4])).await;
        assert_eq!(s, StatusCode::OK);
        let c = claim(&dids[4], 1);
        assert_eq!(v, json!({"cid": c.commit.to_string(), "rev": c.rev.to_string()}));
        for (d, err) in [(&dids[1], "RepoTakendown"), (&dids[2], "RepoDeactivated"), (&dids[3], "RepoSuspended")] {
            let (s, v) = call(&app, &format!("/xrpc/com.atproto.sync.getLatestCommit?did={d}")).await;
            assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some(err)));
        }
        let (_, v) = call(&app, &format!("/xrpc/com.atproto.sync.getLatestCommit?did={}", plc(9999))).await;
        assert_eq!(v["error"], "RepoNotFound");

        let (s, v) = call(&app, "/xrpc/com.atproto.sync.listHosts").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(v, json!({"hosts": [{"hostname": "pds.a", "seq": 77, "accountCount": 5, "status": "active"}]}));
        let (s, v) = call(&app, "/xrpc/com.atproto.sync.getHostStatus?hostname=PDS.A").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(v, json!({"hostname": "pds.a", "seq": 77, "accountCount": 5, "status": "active"}));
        let (s, v) = call(&app, "/xrpc/com.atproto.sync.getHostStatus?hostname=nope.example").await;
        assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("HostNotFound")));
    }
}

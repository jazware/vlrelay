//! A real upstream: an in-process vlpds on its in-memory store, booted the
//! way vlpds's own suite boots a lone node (tests/all/common `spawn_lone`).

use futures::StreamExt;
use serde_json::{Value as J, json};
use std::net::SocketAddr;
use std::time::Duration;
use vlrelay::upstream::frame::{Peek, peek};

pub struct Pds {
    pub addr: SocketAddr,
    pub url: String,
    http: reqwest::Client,
}

pub struct Account {
    pub did: String,
    pub jwt: String,
}

static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

impl Pds {
    pub async fn spawn() -> Pds {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");
        let cfg = vlpds::server::Config {
            dev_mode: true,
            public_url: url.clone(),
            rate_limits_enabled: false,
            ..Default::default()
        };
        let (_app, addr) = vlpds::server::spawn(cfg, listener, None).await.expect("spawn vlpds");
        Pds { addr, url, http: reqwest::Client::new() }
    }

    /// The relay's name for it in dev mode.
    pub fn hostname(&self) -> String {
        self.addr.to_string()
    }

    async fn post(&self, nsid: &str, body: J, jwt: Option<&str>) -> J {
        for _ in 0..600 {
            let mut rb = self.http.post(format!("{}/xrpc/{nsid}", self.url)).json(&body);
            if let Some(j) = jwt {
                rb = rb.bearer_auth(j);
            }
            let r = rb.send().await.unwrap();
            // Argon2 sheds load across the whole test process
            if r.status() == 503 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            let status = r.status();
            let j: J = r.json().await.unwrap_or(J::Null);
            assert!(status.is_success(), "{nsid}: {status} {j}");
            return j;
        }
        panic!("{nsid}: kept shedding");
    }

    pub async fn create_account(&self) -> Account {
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let handle = format!("u{n}x{}.vlpds.test", rand::random::<u16>());
        let j = self
            .post(
                "com.atproto.server.createAccount",
                json!({"handle": handle, "password": "hunter2-password", "email": format!("u{n}@example.com")}),
                None,
            )
            .await;
        Account { did: j["did"].as_str().unwrap().into(), jwt: j["accessJwt"].as_str().unwrap().into() }
    }

    pub async fn post_text(&self, a: &Account, text: &str) {
        let record = json!({"$type": "app.bsky.feed.post", "text": text, "createdAt": "2026-10-04T00:00:00.000Z"});
        self.post(
            "com.atproto.repo.createRecord",
            json!({"repo": a.did, "collection": "app.bsky.feed.post", "record": record}),
            Some(&a.jwt),
        )
        .await;
    }

    /// Every seq on the firehose after `cursor`, read straight off the PDS
    /// until it's quiet.
    pub async fn seqs_after(&self, cursor: i64) -> Vec<i64> {
        let url = format!("ws://{}/xrpc/com.atproto.sync.subscribeRepos?cursor={cursor}", self.addr);
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let mut out = Vec::new();
        while let Ok(Some(Ok(m))) = tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            if let tokio_tungstenite::tungstenite::Message::Binary(b) = m {
                if let Ok(Peek::Message { seq: Some(s), .. }) = peek(&b) {
                    out.push(s);
                }
            }
        }
        out
    }
}

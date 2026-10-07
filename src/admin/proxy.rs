//! Operator identity from a trusted proxy, on the admin listener only
//! (`--admin-listen`; docs/admin-api.md "Sign-in through a proxy"). A proxy
//! in front of that listener (Caddy with Tailscale whois, `tailscale serve`,
//! Cloudflare Access, oauth2-proxy) names the operator in one header; the
//! relay takes it only from `--admin-proxy-from` addresses and only for
//! `--admin-operators` logins, and then treats the request as the admin
//! token would, with that login in the audit trail.
//!
//! The identity is ambient, like a cookie: the operator's browser carries it
//! to whatever page asks. So a request that a browser marks cross-site gets
//! no identity, and an admin API call must come from the console's own
//! origin: `Sec-Fetch-Site: none` (a link opened from mail or chat) is
//! refused too. A write must show it (`Sec-Fetch-Site: same-origin`, or an
//! `Origin` naming this scheme and host). A read is refused only on the
//! browser's word that another site, or no page, asked: curl sends no Fetch
//! Metadata. An `Authorization` header always wins: token auth is unchanged.
//!
//! A kick the console sends on to another member carries the label in the
//! `node:kick` ask, which members take only with the qlog admin token
//! (`node::admin`); no client listener reads it.

use crate::serve::Cidr;
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, HeaderName, Method, Uri, header},
    middleware::Next,
    response::Response,
};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

/// Longest login kept (an email address fits).
const MAX_LOGIN: usize = 256;

#[derive(Clone, Debug)]
pub struct Settings {
    pub header: HeaderName,
    pub from: Vec<Cidr>,
    pub operators: Vec<String>,
}

impl Settings {
    pub fn parse(header: &str, from: &[Cidr], operators: &[String]) -> anyhow::Result<Settings> {
        let name = HeaderName::from_bytes(header.trim().as_bytes())
            .map_err(|_| anyhow::anyhow!("--admin-proxy-header: {header:?} is not a header name"))?;
        let reserved = [
            header::AUTHORIZATION.as_str(),
            header::COOKIE.as_str(),
            header::HOST.as_str(),
            header::ORIGIN.as_str(),
            "sec-fetch-site",
            "x-forwarded-for",
            "x-forwarded-proto",
        ];
        anyhow::ensure!(
            !reserved.contains(&name.as_str()),
            "--admin-proxy-header: {name} is a header vlRelay reads for something else"
        );
        for c in from {
            anyhow::ensure!(c.bits() > 0, "--admin-proxy-from: {c} trusts every address; name the proxy");
        }
        anyhow::ensure!(!from.is_empty(), "--admin-proxy-header needs --admin-proxy-from (the proxy's addresses)");
        let operators: Vec<String> = operators.iter().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        anyhow::ensure!(!operators.is_empty(), "--admin-proxy-header needs --admin-operators (the logins allowed in)");
        if let Some(o) =
            operators.iter().find(|o| o.len() > MAX_LOGIN || o.chars().any(|c| c.is_whitespace() || c.is_control()))
        {
            anyhow::bail!("--admin-operators: {o:?} is not a login (no spaces, at most {MAX_LOGIN} bytes)");
        }
        Ok(Settings { header: name, from: from.to_vec(), operators })
    }

    fn trusts(&self, peer: IpAddr) -> bool {
        self.from.iter().any(|c| c.contains(peer))
    }
}

/// Request extension: what the admin listener made of the proxy's header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyIdentity {
    Operator(Arc<str>),
    /// A trusted proxy named someone, but not one the request may act as.
    Refused(String),
}

/// The admin listener's verdict on one request. None: no identity (no
/// header, a peer outside `--admin-proxy-from`, or an Authorization header,
/// which token auth answers).
pub fn identify(
    s: &Settings,
    peer: Option<IpAddr>,
    headers: &HeaderMap,
    method: &Method,
    uri: &Uri,
) -> Option<ProxyIdentity> {
    let raw = headers.get(&s.header)?;
    if headers.contains_key(header::AUTHORIZATION) {
        return None;
    }
    if !peer.is_some_and(|p| s.trusts(p.to_canonical())) {
        tracing::debug!(peer = ?peer, header = %s.header, "admin proxy header from an untrusted peer: ignored");
        return None;
    }
    if headers.get_all(&s.header).iter().count() > 1 {
        return Some(ProxyIdentity::Refused(format!("more than one {} header", s.header)));
    }
    let login = raw.to_str().map(str::trim).unwrap_or("");
    if login.is_empty() || login.len() > MAX_LOGIN || login.chars().any(char::is_control) {
        return Some(ProxyIdentity::Refused(format!("{} is not a login", s.header)));
    }
    if !s.operators.iter().any(|o| o == login) {
        return Some(ProxyIdentity::Refused(format!("{login} is not an operator here")));
    }
    if let Err(why) = same_origin(headers, method, uri) {
        return Some(ProxyIdentity::Refused(why));
    }
    Some(ProxyIdentity::Operator(login.into()))
}

/// Writes need the browser's word that the console's own origin sent them.
/// Reads are refused only on its word that another site did, or that no
/// page did (`none`, a link opened from elsewhere). A non-browser client
/// sends no Fetch Metadata, so its reads pass and its writes don't (scripts
/// use the admin token).
fn same_origin(headers: &HeaderMap, method: &Method, uri: &Uri) -> Result<(), String> {
    let site = headers.get("sec-fetch-site").map(|v| v.to_str().unwrap_or("?"));
    let origin = headers.get(header::ORIGIN).map(|v| v.to_str().unwrap_or("?"));
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    let cross = |why: String| Err(format!("cross-site request refused ({why}); use the console's own origin"));
    let api = uri.path().starts_with("/admin/api/");
    if let Some(site) = site.filter(|s| *s != "same-origin" && !(*s == "none" && !api)) {
        return cross(format!("Sec-Fetch-Site: {site}"));
    }
    if let Some(o) = origin.filter(|o| !origin_is_host(o, scheme(headers), host)) {
        return cross(format!("Origin: {o}"));
    }
    let read = matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS);
    if !read && site != Some("same-origin") && origin.is_none() {
        return cross("a write without Sec-Fetch-Site: same-origin or Origin".into());
    }
    Ok(())
}

/// The scheme the client used: the trusted proxy's `X-Forwarded-Proto` (only
/// a peer in `--admin-proxy-from` gets this far), else the listener's own
/// plain http.
fn scheme(headers: &HeaderMap) -> &str {
    match headers.get("x-forwarded-proto").and_then(|v| v.to_str().ok()).map(str::trim) {
        Some("https") => "https",
        _ => "http",
    }
}

/// `Origin` is `scheme://host[:port]`; the proxy passes the client's `Host`.
fn origin_is_host(origin: &str, scheme: &str, host: Option<&str>) -> bool {
    let Some(host) = host else { return false };
    let Some((o_scheme, authority)) = origin.split_once("://") else { return false };
    if o_scheme != scheme {
        return false;
    }
    let default = if scheme == "https" { ":443" } else { ":80" };
    let strip = |a: &str| a.strip_suffix(default).unwrap_or(a).to_ascii_lowercase();
    strip(authority) == strip(host)
}

/// The admin listener's outer layer: the proxy's header becomes a
/// [`ProxyIdentity`] extension and never reaches anything else.
pub async fn layer(State(s): State<Arc<Settings>>, mut req: Request, next: Next) -> Response {
    let peer = req.extensions().get::<ConnectInfo<SocketAddr>>().map(|c| c.0.ip());
    let id = identify(&s, peer, req.headers(), req.method(), req.uri());
    req.headers_mut().remove(&s.header);
    if let Some(id) = id {
        req.extensions_mut().insert(id);
    }
    next.run(req).await
}

/// `--admin-listen`'s router: what `--listen` serves, plus the proxy's
/// header when `--admin-proxy-header` is set (else the token alone).
pub fn admin_listener(app: axum::Router, proxy: Option<Arc<Settings>>) -> axum::Router {
    match proxy {
        Some(s) => app.layer(axum::middleware::from_fn_with_state(s, layer)),
        None => app,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn cidrs(s: &[&str]) -> Vec<Cidr> {
        s.iter().map(|c| c.parse().unwrap()).collect()
    }

    fn settings() -> Settings {
        Settings::parse("Tailscale-User-Login", &cidrs(&["172.18.0.10/32", "127.0.0.1"]), &[ALICE.into()]).unwrap()
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::HOST, HeaderValue::from_static("relay-admin.example.com"));
        for (k, v) in pairs {
            h.append(HeaderName::from_bytes(k.as_bytes()).unwrap(), HeaderValue::from_str(v).unwrap());
        }
        h
    }

    const PROXY: Option<IpAddr> = Some(IpAddr::V4(std::net::Ipv4Addr::new(172, 18, 0, 10)));
    const ALICE: &str = "alice@example.com";
    const SESSION: &str = "/admin/api/session";

    fn op() -> Option<ProxyIdentity> {
        Some(ProxyIdentity::Operator(ALICE.into()))
    }

    fn uri(u: &str) -> Uri {
        u.parse().unwrap()
    }

    #[test]
    fn settings_refuse_what_cannot_work() {
        let ops = ["a@example.com".to_string()];
        let from = cidrs(&["10.0.0.0/8"]);
        assert!(Settings::parse("Authorization", &from, &ops).is_err());
        assert!(Settings::parse("X-Forwarded-Proto", &from, &ops).is_err());
        assert!(Settings::parse("bad header", &from, &ops).is_err());
        assert!(Settings::parse("X-Login", &cidrs(&["0.0.0.0/0"]), &ops).is_err());
        assert!(Settings::parse("X-Login", &cidrs(&["::/0"]), &ops).is_err());
        assert!(Settings::parse("X-Login", &[], &ops).is_err());
        assert!(Settings::parse("X-Login", &from, &[" ".into()]).is_err());
        assert!(Settings::parse("X-Login", &from, &["admin (token)".into()]).is_err());
        assert!(Settings::parse("X-Login", &from, &ops).is_ok());
    }

    #[test]
    fn trusted_peer_and_allowlist() {
        let s = settings();
        let get = |peer, h: &HeaderMap| identify(&s, peer, h, &Method::GET, &uri(SESSION));
        let h = headers(&[("tailscale-user-login", ALICE)]);
        assert_eq!(get(PROXY, &h), op());
        // untrusted peers, or none known: the header means nothing. The
        // proxy's own /32 leaves the network's gateway and neighbors out.
        for p in ["100.64.0.9", "172.18.0.1", "172.18.0.2"] {
            assert_eq!(get(Some(p.parse().unwrap()), &h), None, "{p}");
        }
        assert_eq!(get(None, &h), None);
        // the TCP peer decides, never a forwarded address it claims
        let xff = headers(&[("tailscale-user-login", ALICE), ("x-forwarded-for", "172.18.0.10")]);
        assert_eq!(get(Some("100.64.0.9".parse().unwrap()), &xff), None);
        // an IPv4-mapped peer is its IPv4 address
        assert_eq!(get(Some("::ffff:127.0.0.1".parse().unwrap()), &h), op());
        // exact compare: no case folding, no prefixes
        for login in ["mallory@example.com", "ALICE@example.com", "alice@example.com.evil", "alice"] {
            let h = headers(&[("tailscale-user-login", login)]);
            assert!(matches!(get(PROXY, &h), Some(ProxyIdentity::Refused(_))), "{login}");
        }
        for empty in ["", "  "] {
            let h = headers(&[("tailscale-user-login", empty)]);
            assert!(matches!(get(PROXY, &h), Some(ProxyIdentity::Refused(_))), "{empty:?}");
        }
        let two = headers(&[("tailscale-user-login", ALICE), ("tailscale-user-login", ALICE)]);
        assert!(matches!(get(PROXY, &two), Some(ProxyIdentity::Refused(_))));
        // token auth answers a request that brings one
        let tok = headers(&[("tailscale-user-login", ALICE), ("authorization", "Basic x")]);
        assert_eq!(identify(&s, PROXY, &tok, &Method::POST, &uri(SESSION)), None);
        assert_eq!(get(PROXY, &headers(&[])), None);
    }

    #[test]
    fn cross_site_requests_get_no_identity() {
        let s = settings();
        let id = |pairs: &[(&str, &str)], m: Method, u: &str| {
            let mut all = vec![("tailscale-user-login", ALICE), ("x-forwarded-proto", "https")];
            all.extend_from_slice(pairs);
            identify(&s, PROXY, &headers(&all), &m, &uri(u))
        };
        let refused = |r: Option<ProxyIdentity>| matches!(r, Some(ProxyIdentity::Refused(_)));
        let w = "/admin/api/hosts/pds.example.com/action";
        // writes: only with evidence of the console's own origin
        assert_eq!(id(&[("sec-fetch-site", "same-origin")], Method::POST, w), op());
        assert_eq!(id(&[("origin", "https://relay-admin.example.com")], Method::POST, w), op());
        assert_eq!(id(&[("origin", "https://RELAY-admin.example.com:443")], Method::PUT, w), op());
        assert!(refused(id(&[], Method::POST, w)));
        assert!(refused(id(&[], Method::DELETE, w)));
        assert!(refused(id(&[("sec-fetch-site", "none")], Method::POST, w)));
        assert!(refused(id(&[("sec-fetch-site", "cross-site")], Method::POST, w)));
        assert!(refused(id(&[("sec-fetch-site", "same-site")], Method::POST, w)));
        assert!(refused(id(&[("origin", "https://evil.example.net")], Method::POST, w)));
        assert!(refused(id(&[("origin", "null")], Method::POST, w)));
        // the scheme must match too: the proxy said https
        assert!(refused(id(&[("origin", "http://relay-admin.example.com")], Method::POST, w)));
        assert!(refused(id(
            &[("sec-fetch-site", "same-origin"), ("origin", "https://evil.example.net")],
            Method::POST,
            w
        )));
        // reads: refused only when the browser says another site, or no page, asked
        assert_eq!(id(&[], Method::GET, SESSION), op());
        assert_eq!(id(&[("sec-fetch-site", "same-origin")], Method::GET, SESSION), op());
        assert!(refused(id(&[("sec-fetch-site", "none")], Method::GET, SESSION)));
        assert!(refused(id(&[("sec-fetch-site", "cross-site")], Method::GET, SESSION)));
        assert!(refused(id(&[("sec-fetch-site", "same-site")], Method::GET, SESSION)));
        assert!(refused(id(&[("origin", "https://evil.example.net")], Method::GET, SESSION)));
        // the console's pages themselves open from the address bar
        assert_eq!(id(&[("sec-fetch-site", "none")], Method::GET, "/admin/hosts"), op());
    }

    /// The routers both listeners serve, over the demo relay: what `--listen`
    /// serves, and `--admin-listen` with the proxy trusted at `from`.
    mod listeners {
        use super::*;
        use crate::admin::{AdminSource, demo::Demo};
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use serde_json::{Value, json};
        use tower::ServiceExt;

        const TOKEN: &str = "t";
        const H: &str = "tailscale-user-login";
        const HOST: &str = "relay-admin.example.com";
        const PEER: &str = "127.0.0.1:40000";

        struct Relay {
            demo: Arc<Demo>,
            public: axum::Router,
            admin: axum::Router,
        }

        fn relay(from: &str) -> Relay {
            let demo = Demo::start(7);
            let public = crate::admin::api_routes(demo.clone(), TOKEN.into());
            let s = Settings::parse("Tailscale-User-Login", &cidrs(&[from]), &[ALICE.into()]).unwrap();
            let admin = admin_listener(public.clone(), Some(Arc::new(s)));
            Relay { demo, public, admin }
        }

        async fn send(
            app: &axum::Router,
            method: &str,
            path: &str,
            hs: &[(&str, &str)],
            body: Option<Value>,
        ) -> (StatusCode, Value) {
            let mut b = Request::builder().method(method).uri(path).header("host", HOST);
            for (k, v) in hs {
                b = b.header(*k, *v);
            }
            let body = match body {
                Some(v) => {
                    b = b.header("content-type", "application/json");
                    Body::from(serde_json::to_vec(&v).unwrap())
                }
                None => Body::empty(),
            };
            let mut req = b.body(body).unwrap();
            req.extensions_mut().insert(ConnectInfo::<SocketAddr>(PEER.parse().unwrap()));
            let r = app.clone().oneshot(req).await.unwrap();
            let status = r.status();
            let b = axum::body::to_bytes(r.into_body(), 1 << 24).await.unwrap();
            (status, serde_json::from_slice(&b).unwrap_or(Value::Null))
        }

        /// `Basic admin:t` and `Basic admin:wrong`.
        fn basic(token: &str) -> String {
            match token {
                TOKEN => "Basic YWRtaW46dA==".into(),
                _ => "Basic YWRtaW46d3Jvbmc=".into(),
            }
        }

        const SESSION: &str = "/admin/api/session";

        #[tokio::test]
        async fn header_counts_on_the_admin_listener_only() {
            let r = relay("127.0.0.0/8");
            let (st, v) = send(&r.admin, "GET", SESSION, &[(H, ALICE)], None).await;
            assert_eq!((st, v), (StatusCode::OK, json!({"auth": "proxy", "operator": ALICE})));
            // the public listener never reads it, even from a trusted address
            let (st, v) = send(&r.public, "GET", SESSION, &[(H, ALICE)], None).await;
            assert_eq!((st, v["error"].as_str()), (StatusCode::UNAUTHORIZED, Some("AuthenticationRequired")));
            assert_eq!(send(&r.admin, "GET", SESSION, &[], None).await.0, StatusCode::UNAUTHORIZED);

            // a login off the allowlist is refused, and two headers, and an empty one
            for hs in [&[(H, "mallory@example.com")][..], &[(H, ALICE), (H, "mallory@example.com")], &[(H, "")]] {
                let (st, v) = send(&r.admin, "GET", SESSION, hs, None).await;
                assert_eq!((st, v["error"].as_str()), (StatusCode::FORBIDDEN, Some("OperatorRefused")), "{hs:?}");
            }

            // token auth is unchanged, on both listeners, and wins over the header
            let tok = basic(TOKEN);
            let wrong = basic("wrong");
            for app in [&r.public, &r.admin] {
                let (st, v) = send(app, "GET", SESSION, &[("authorization", &tok)], None).await;
                assert_eq!((st, v), (StatusCode::OK, json!({"auth": "token"})));
                let hs = [("authorization", tok.as_str()), (H, "mallory@example.com")];
                assert_eq!(send(app, "GET", SESSION, &hs, None).await.1, json!({"auth": "token"}));
                let hs = [("authorization", wrong.as_str()), (H, ALICE)];
                assert_eq!(send(app, "GET", SESSION, &hs, None).await.0, StatusCode::UNAUTHORIZED);
            }
        }

        #[tokio::test]
        async fn header_from_an_untrusted_peer_is_ignored() {
            let r = relay("10.0.0.1/32");
            assert_eq!(send(&r.admin, "GET", SESSION, &[(H, ALICE)], None).await.0, StatusCode::UNAUTHORIZED);
            // nor does the address it claims to forward for count
            let hs = [(H, ALICE), ("x-forwarded-for", "10.0.0.1")];
            assert_eq!(send(&r.admin, "GET", SESSION, &hs, None).await.0, StatusCode::UNAUTHORIZED);
            let tok = basic(TOKEN);
            assert_eq!(send(&r.admin, "GET", SESSION, &[("authorization", &tok)], None).await.0, StatusCode::OK);
        }

        #[tokio::test]
        async fn admin_listener_without_proxy_settings_is_token_only() {
            let demo = Demo::start(7);
            let admin = admin_listener(crate::admin::api_routes(demo, TOKEN.into()), None);
            assert_eq!(send(&admin, "GET", SESSION, &[(H, ALICE)], None).await.0, StatusCode::UNAUTHORIZED);
            let tok = basic(TOKEN);
            assert_eq!(send(&admin, "GET", SESSION, &[("authorization", &tok)], None).await.0, StatusCode::OK);
        }

        #[tokio::test]
        async fn cross_site_writes_are_refused_and_the_audit_trail_names_the_operator() {
            let r = relay("127.0.0.1/32");
            let rules = "/admin/api/domain-rules";
            let rule = |p: &str| Some(json!({"pattern": p, "effect": {"kind": "ban"}, "note": "test"}));
            let origin = format!("http://{HOST}");
            let refused: [&[(&str, &str)]; 6] = [
                &[],
                &[("sec-fetch-site", "cross-site")],
                &[("sec-fetch-site", "same-site")],
                &[("sec-fetch-site", "none")],
                &[("origin", "http://evil.example.net")],
                // the scheme too: no X-Forwarded-Proto, so the listener's own http
                &[("origin", "https://relay-admin.example.com")],
            ];
            for extra in refused {
                let mut hs = vec![(H, ALICE)];
                hs.extend_from_slice(extra);
                let (st, v) = send(&r.admin, "POST", rules, &hs, rule("refused.example.com")).await;
                assert_eq!((st, v["error"].as_str()), (StatusCode::FORBIDDEN, Some("OperatorRefused")), "{extra:?}");
            }
            // a read the browser marks cross-site, or that no page made (a link opened from mail or chat), too
            for site in ["cross-site", "none"] {
                let hs = [(H, ALICE), ("sec-fetch-site", site)];
                assert_eq!(send(&r.admin, "GET", rules, &hs, None).await.0, StatusCode::FORBIDDEN, "{site}");
            }
            // curl's read, with no Fetch Metadata, passes
            assert_eq!(send(&r.admin, "GET", rules, &[(H, ALICE)], None).await.0, StatusCode::OK);

            let same = [(H, ALICE), ("sec-fetch-site", "same-origin")];
            let (st, a) = send(&r.admin, "POST", rules, &same, rule("a.example.com")).await;
            assert_eq!((st, a["createdBy"].as_str()), (StatusCode::OK, Some("alice@example.com (proxy)")));
            let hs = [(H, ALICE), ("origin", origin.as_str())];
            let (st, b) = send(&r.admin, "POST", rules, &hs, rule("b.example.com")).await;
            assert_eq!((st, b["createdBy"].as_str()), (StatusCode::OK, Some("alice@example.com (proxy)")));

            // the token needs no origin checks, and says so: a token caller isn't an operator,
            // whatever header it sends along
            let tok = basic(TOKEN);
            let hs = [("authorization", tok.as_str()), (H, ALICE)];
            let (st, c) = send(&r.admin, "POST", rules, &hs, rule("c.example.com")).await;
            assert_eq!((st, c["createdBy"].as_str()), (StatusCode::OK, Some("admin (token)")));
            let made: Vec<String> = r.demo.domain_rules().await.unwrap().into_iter().map(|r| r.pattern).collect();
            assert!(!made.iter().any(|p| p == "refused.example.com"), "{made:?}");
        }
    }

    #[test]
    fn scheme_comes_from_the_proxy_else_plain_http() {
        assert!(origin_is_host("http://h:8080", scheme(&HeaderMap::new()), Some("h:8080")));
        assert!(!origin_is_host("https://h", scheme(&HeaderMap::new()), Some("h")));
        let mut tls = HeaderMap::new();
        tls.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert!(origin_is_host("https://h", scheme(&tls), Some("h:443")));
        assert!(!origin_is_host("http://h", scheme(&tls), Some("h")));
        assert!(!origin_is_host("ftp://h", scheme(&tls), Some("h")));
        assert!(!origin_is_host("https://h", scheme(&tls), None));
    }
}

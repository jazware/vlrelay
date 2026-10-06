//! Serves the built dashboard (`ui/dist`): `/admin` and everything under it
//! that isn't `/admin/api`, and `/docs/*`, get index.html; `/assets/*` and
//! `/fonts/*` are files.

use axum::{
    Router,
    body::Bytes,
    extract::{Path as AxPath, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use std::{collections::HashMap, path::Path, sync::Arc};

const PLACEHOLDER: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>vlRelay</title></head>\
<body><p>No dashboard build found. Run <code>npm run build</code> in <code>ui</code>, or pass <code>--ui-dir</code>.</p></body></html>";

/// Scripts, styles and API calls from this origin only. Inline style
/// attributes set through the CSSOM (React `style`, uPlot) aren't governed by style-src.
const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'";

/// The build, read whole at startup so only names found here are servable.
pub struct UiFiles {
    files: HashMap<String, (Bytes, HeaderValue)>,
    index: Bytes,
}

impl UiFiles {
    /// `dir` unset: this source tree's `ui/dist` if built, else a placeholder.
    pub fn load(dir: Option<&Path>) -> anyhow::Result<UiFiles> {
        let dir = match dir {
            Some(d) => d.to_path_buf(),
            None => {
                let dev = Path::new(env!("CARGO_MANIFEST_DIR")).join("ui/dist");
                if !dev.join("index.html").is_file() {
                    tracing::warn!(dir = %dev.display(), "no built dashboard: serving a placeholder");
                    return Ok(UiFiles { files: HashMap::new(), index: Bytes::from_static(PLACEHOLDER.as_bytes()) });
                }
                dev
            }
        };
        let mut files = HashMap::new();
        walk(&dir, &dir, &mut files)?;
        let index = files
            .get("index.html")
            .map(|(b, _)| b.clone())
            .ok_or_else(|| anyhow::anyhow!("{}: no index.html", dir.display()))?;
        tracing::info!(dir = %dir.display(), files = files.len(), "dashboard loaded");
        Ok(UiFiles { files, index })
    }
}

fn walk(root: &Path, dir: &Path, out: &mut HashMap<String, (Bytes, HeaderValue)>) -> anyhow::Result<()> {
    for e in std::fs::read_dir(dir)? {
        let path = e?.path();
        if path.is_dir() {
            walk(root, &path, out)?;
        } else if path.is_file() {
            let rel = path
                .strip_prefix(root)?
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let mime = mime_guess::from_path(&path).first_or_octet_stream();
            out.insert(rel, (std::fs::read(&path)?.into(), HeaderValue::from_str(mime.as_ref())?));
        }
    }
    Ok(())
}

pub fn ui_routes(ui: Arc<UiFiles>) -> Router {
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/admin") }))
        .route("/admin", get(shell))
        .route("/admin/", get(shell))
        .route("/admin/{*rest}", get(shell))
        .with_state(ui.clone())
        .merge(docs_routes(ui))
}

/// The docs site and the files it loads. Public, so a node serves it even
/// without `--admin-token`: the shell is static, and only `/admin/api` holds data.
pub fn docs_routes(ui: Arc<UiFiles>) -> Router {
    Router::new()
        .route("/docs", get(shell))
        .route("/docs/", get(shell))
        .route("/docs/{*rest}", get(shell))
        .route("/assets/{*path}", get(asset))
        .route("/fonts/{*path}", get(asset))
        .route("/favicon.svg", get(asset))
        .with_state(ui)
}

async fn shell(State(ui): State<Arc<UiFiles>>, uri: axum::http::Uri) -> Response {
    // an unknown API path is a JSON 404, not the SPA
    if uri.path().starts_with("/admin/api/") {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": "NotFound", "message": "no such endpoint"})),
        )
            .into_response();
    }
    (
        [
            (header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8")),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
            (header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP)),
            (header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff")),
        ],
        ui.index.clone(),
    )
        .into_response()
}

async fn asset(State(ui): State<Arc<UiFiles>>, uri: axum::http::Uri, _p: Option<AxPath<String>>) -> Response {
    let key = uri.path().trim_start_matches('/');
    match ui.files.get(key) {
        Some((data, mime)) => {
            // hashed names under assets/ never change content
            let cache =
                if key.starts_with("assets/") { "public, max-age=31536000, immutable" } else { "public, max-age=3600" };
            (
                [(header::CONTENT_TYPE, mime.clone()), (header::CACHE_CONTROL, HeaderValue::from_static(cache))],
                data.clone(),
            )
                .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

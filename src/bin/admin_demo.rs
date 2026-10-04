//! Serves the operator dashboard against a simulated busy relay, for UI work
//! and screenshots: `cargo run --bin admin_demo -- --listen 127.0.0.1:2790`.

use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use vlrelay::admin::{self, UiFiles, demo::Demo};

#[derive(Parser)]
#[command(about = "vlRelay operator dashboard on simulated data")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:2790", env = "VLRELAY_ADMIN_DEMO_LISTEN")]
    listen: SocketAddr,
    /// A built `ui/dist` (default: this source tree's, if built).
    #[arg(long)]
    ui_dir: Option<PathBuf>,
    #[arg(long, default_value = "demo", env = "VLRELAY_ADMIN_TOKEN")]
    admin_token: String,
    /// Seeds the simulation (same seed, same hosts).
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();
    let ui = Arc::new(UiFiles::load(args.ui_dir.as_deref())?);
    let app = admin::app(Demo::start(args.seed), args.admin_token, ui);
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(addr = %args.listen, "admin demo: open http://{}/admin", args.listen);
    axum::serve(listener, app).await?;
    Ok(())
}

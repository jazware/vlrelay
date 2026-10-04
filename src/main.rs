fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "vlrelay");
    Ok(())
}

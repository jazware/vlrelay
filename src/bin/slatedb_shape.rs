//! Read-only: a SlateDB's latest manifest and compactions as JSON, from a
//! local directory (`--dir`) or an S3-compatible bucket named by the
//! environment (R2_ENDPOINT, R2_BUCKET, R2_ACCESS_KEY_ID,
//! R2_SECRET_ACCESS_KEY). Only GETs and LISTs.
//!
//!   slatedb_shape --path vlrelay-example-dev/plc/seeds --out shape.json

use clap::Parser;
use std::sync::Arc;

#[derive(Parser)]
struct Cli {
    /// The database's path in the store.
    #[arg(long)]
    path: String,
    /// A local directory standing in for the bucket.
    #[arg(long)]
    dir: Option<std::path::PathBuf>,
    #[arg(long)]
    out: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Cli::parse();
    let store: Arc<dyn object_store::ObjectStore> = match &a.dir {
        Some(d) => Arc::new(object_store::local::LocalFileSystem::new_with_prefix(d)?),
        None => {
            let env = |k: &str| std::env::var(k).map_err(|_| anyhow::anyhow!("{k} unset"));
            Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_endpoint(env("R2_ENDPOINT")?)
                    .with_bucket_name(env("R2_BUCKET")?)
                    .with_access_key_id(env("R2_ACCESS_KEY_ID")?)
                    .with_secret_access_key(env("R2_SECRET_ACCESS_KEY")?)
                    .with_region("auto")
                    .build()?,
            )
        }
    };
    let admin = slatedb::admin::Admin::builder(a.path.as_str(), store).build();
    let manifest = admin.read_manifest(None).await?;
    let compactions = admin.read_compactions(None).await?;
    let out = serde_json::json!({ "manifest": manifest, "compactions": compactions });
    std::fs::write(&a.out, serde_json::to_vec_pretty(&out)?)?;
    eprintln!("wrote {}", a.out.display());
    Ok(())
}

//! Dumps the quorum log's flushed entries for one DID (or all): seq, epoch,
//! the frame's rev, prevData and commit, and the entry's meta.
//!
//!   cargo run --example qlog_dump -- http://127.0.0.1:3590 PREFIX [DID]

use vlatproto::cbor::ValueRef;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let s3 = vlsync_store::store::S3Config {
        endpoint: args[1].clone(),
        bucket: "vlrelay".into(),
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        region: "us-east-1".into(),
    };
    let store = vlsync_store::store::Store::s3(&s3, &args[2], None, 8)?;
    let want = args.get(3).cloned();
    let mut ord = 0;
    while let Some(es) = vlrelay::qlog::flush::read_entries(&store, ord).await? {
        for e in es {
            let (hdr, n) = ValueRef::decode_prefix(&e.data)?;
            let body = ValueRef::decode(&e.data[n..])?;
            let t = hdr.get("t").and_then(|v| v.as_str()).unwrap_or("?").to_string();
            let did = body.get("repo").or_else(|| body.get("did")).and_then(|v| v.as_str()).unwrap_or("").to_string();
            if want.as_ref().is_some_and(|w| *w != did) {
                continue;
            }
            let rev = body.get("rev").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let link = |k: &str| match body.get(k) {
                Some(ValueRef::Link(c)) => c.to_string(),
                _ => "-".into(),
            };
            let m = vlrelay::qlog::log::Meta::decode(&e.meta).unwrap_or_default();
            let x = &m.ext;
            let (host, useq) = if x.len() > 4 {
                let hl = u16::from_be_bytes([x[2], x[3]]) as usize;
                let h = String::from_utf8_lossy(&x[4..4 + hl]).into_owned();
                let u = i64::from_be_bytes(x[4 + hl..12 + hl].try_into()?);
                (h, u)
            } else {
                (String::new(), 0)
            };
            println!(
                "{} e{} {t} {did} rev={rev} prevData={} commit={} writes={} host={host} useq={useq} upseq={}",
                e.seq,
                e.epoch,
                link("prevData"),
                link("commit"),
                m.writes.len(),
                match body.get("seq") {
                    Some(ValueRef::Int(i)) => *i,
                    _ => -1,
                }
            );
        }
        ord += 1;
    }
    Ok(())
}

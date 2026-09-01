//! `verify`: download every chunk of a snapshot and re-hash it. Writes
//! nothing and needs no ZFS — run it anywhere with the credentials.

use anyhow::bail;

use crate::manifest::keys;

use super::{Conn, target};

pub async fn run(snapshot: &str, uri: &str, conn: &Conn) -> anyhow::Result<()> {
    let t = target(uri, conn)?;
    let m = t.manifest_for(snapshot).await?;
    let mut stream_hash = blake3::Hasher::new();
    let mut total = 0u64;
    let started = std::time::Instant::now();
    for c in &m.chunks {
        let key = keys::chunk(&t.prefix, m.dataset_guid, m.snapshot_guid, c.seq);
        let data = t.store.get(&key).await?;
        if data.len() as u64 != c.bytes {
            bail!(
                "{key}: {} bytes in the store, manifest says {}",
                data.len(),
                c.bytes
            );
        }
        let got = blake3::hash(&data).to_hex().to_string();
        if got != c.blake3 {
            bail!("{key}: BLAKE3 mismatch (stored data is corrupt)");
        }
        stream_hash.update_rayon(&data);
        total += c.bytes;
    }
    if total != m.bytes {
        bail!("chunks sum to {total} bytes, manifest says {}", m.bytes);
    }
    let got = stream_hash.finalize().to_hex().to_string();
    if got != m.stream_blake3 {
        bail!("whole-stream BLAKE3 mismatch");
    }
    let secs = started.elapsed().as_secs_f64();
    println!(
        "{snapshot} verified: {} bytes, {} chunks, {:.1}s ({:.1} MB/s)",
        total,
        m.chunks.len(),
        secs,
        total as f64 / secs / 1e6
    );
    Ok(())
}

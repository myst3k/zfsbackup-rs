//! `receive`: restore a snapshot and the chain it depends on.
//!
//! Streams are applied oldest-first (the full, then each incremental) with
//! one `zfs receive` per stream; chunks are prefetched `window` ahead and
//! BLAKE3-verified before a byte reaches zfs.

use anyhow::{Context, bail};
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::manifest::{Manifest, keys};
use crate::zfs::Zfs;

use super::{Target, target};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    snapshot: &str,
    uri: &str,
    to: &str,
    force: bool,
    window: usize,
    endpoint: Option<&str>,
    region: Option<&str>,
    zfs_bin: &str,
) -> anyhow::Result<()> {
    let t = target(uri, endpoint, region)?;
    let zfs = Zfs::new().with_binary(zfs_bin);
    let all = t.manifests().await?;
    let tip = all
        .iter()
        .find(|m| m.snapshot == snapshot)
        .with_context(|| format!("{snapshot} is not archived here (see `list`)"))?;

    // Walk from_guid links back to the full.
    let mut chain: Vec<&Manifest> = vec![tip];
    while let Some(from) = chain.last().expect("non-empty").from_guid {
        let base = all
            .iter()
            .find(|m| m.snapshot_guid == from)
            .with_context(|| {
                format!(
                    "chain is broken: base {from} of {} is not archived",
                    chain.last().expect("non-empty").snapshot
                )
            })?;
        chain.push(base);
    }
    chain.reverse();
    println!(
        "restoring {} stream(s) into {to} ({} bytes total)",
        chain.len(),
        chain.iter().map(|m| m.bytes).sum::<u64>()
    );

    let started = std::time::Instant::now();
    let mut total = 0u64;
    for m in &chain {
        let mut proc = zfs.receive(to, force).await?;
        let mut stdin = proc.stdin.take().context("zfs receive has no stdin")?;
        let fetches = futures::stream::iter(m.chunks.iter().map(|c| {
            let key = keys::chunk(&t.prefix, m.dataset_guid, m.snapshot_guid, c.seq);
            let store = t.store.clone();
            let want = c.blake3.clone();
            let bytes = c.bytes;
            async move {
                let data = store.get(&key).await?;
                if data.len() as u64 != bytes {
                    bail!(
                        "{key}: {} bytes in store, manifest says {bytes}",
                        data.len()
                    );
                }
                if blake3::hash(&data).to_hex().to_string() != want {
                    bail!("{key}: BLAKE3 mismatch (stored data is corrupt)");
                }
                Ok::<_, anyhow::Error>(data)
            }
        }))
        .buffered(window.max(1));
        futures::pin_mut!(fetches);
        while let Some(data) = fetches.next().await {
            let data = data?;
            stdin.write_all(&data).await?;
            total += data.len() as u64;
        }
        stdin.flush().await?;
        drop(stdin);
        proc.finish().await?;
        println!("  applied {}", m.snapshot);
    }
    let secs = started.elapsed().as_secs_f64();
    println!(
        "restored {} into {to}: {} bytes in {:.1}s ({:.1} MB/s)",
        snapshot,
        total,
        secs,
        total as f64 / secs / 1e6
    );
    Ok(())
}

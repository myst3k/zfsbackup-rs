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

use super::target;

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
    let tip_guid = super::pick(all.clone(), snapshot)?.snapshot_guid;
    let tip = all
        .iter()
        .find(|m| m.snapshot_guid == tip_guid)
        .context("selected manifest vanished between listings")?;

    // Walk from_guid links back to the full.
    let mut chain: Vec<&Manifest> = vec![tip];
    let mut cur = tip;
    while let Some(from) = cur.from_guid {
        let base = all
            .iter()
            .find(|m| m.snapshot_guid == from)
            .with_context(|| {
                format!(
                    "chain is broken: base {from} of {} is not archived",
                    cur.snapshot
                )
            })?;
        chain.push(base);
        cur = base;
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
        match apply(&t, &zfs, m, to, force, window, &mut total).await {
            Ok(()) => println!("  applied {}", m.snapshot),
            Err(e) => {
                // `zfs receive -s` leaves resumable state behind on failure,
                // which blocks every later receive into this dataset. Clear
                // it so a retry starts from a clean target.
                if let Err(ae) = zfs.receive_abort(to).await {
                    tracing::warn!(error = %ae, "could not clear partial receive state on {to}; run `zfs receive -A {to}` before retrying");
                }
                return Err(e.context(format!("restoring {} into {to}", m.snapshot)));
            }
        }
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

/// Stream one archived snapshot into `zfs receive`.
async fn apply(
    t: &super::Target,
    zfs: &Zfs,
    m: &Manifest,
    to: &str,
    force: bool,
    window: usize,
    total: &mut u64,
) -> anyhow::Result<()> {
    {
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
            *total += data.len() as u64;
        }
        stdin.flush().await?;
        drop(stdin);
        proc.finish().await?;
    }
    Ok(())
}

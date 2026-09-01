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

use super::{Conn, target};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    snapshot: &str,
    uri: &str,
    to: &str,
    force: bool,
    window: usize,
    conn: &Conn,
    zfs_bin: &str,
) -> anyhow::Result<()> {
    let t = target(uri, conn)?;
    let zfs = Zfs::new().with_binary(zfs_bin);
    let all = t.manifests().await?;
    let tip_guid = super::pick(all.clone(), snapshot)?.snapshot_guid;
    let tip = all
        .iter()
        .find(|m| m.snapshot_guid == tip_guid)
        .context("selected manifest vanished between listings")?;

    // Walk from_guid links back to the full. `seen` guards against a cycle
    // in hand-edited or corrupted manifests, which would otherwise loop
    // forever building an unbounded chain.
    let mut chain: Vec<&Manifest> = vec![tip];
    let mut seen: std::collections::HashSet<crate::types::Guid> =
        std::iter::once(tip.snapshot_guid).collect();
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
        if !seen.insert(from) {
            bail!(
                "chain of {} is circular at {} ({}); the manifests are inconsistent",
                snapshot,
                base.snapshot,
                from
            );
        }
        chain.push(base);
        cur = base;
    }
    chain.reverse();
    // The chunk size is whatever the sender used; it, not a receive-side
    // flag, sets how much memory `--window` costs here.
    let chunk = chain
        .iter()
        .flat_map(|m| m.chunks.first())
        .map(|c| c.bytes)
        .max()
        .unwrap_or(0);
    println!(
        "restoring {} stream(s) into {to} ({} bytes total; {} chunks of up to {:.0} MiB, {} prefetched)",
        chain.len(),
        chain.iter().map(|m| m.bytes).sum::<u64>(),
        chain.iter().map(|m| m.chunks.len()).sum::<usize>(),
        chunk as f64 / 1_048_576.0,
        window.max(1)
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
    // `zfs receive -u` keeps the dataset unmounted so the restore needs no
    // root. Say so: an unmounted dataset looks exactly like an empty one.
    println!("{to} is not mounted yet — `zfs mount {to}` (needs root)");
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
                // Retried: a transient 5xx partway through a long restore
                // would otherwise discard everything applied so far.
                let data = super::retry("fetch chunk", || {
                    let store = store.clone();
                    let key = key.clone();
                    async move { Ok(store.get(&key).await?) }
                })
                .await?;
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
            // A write failure means zfs exited; collecting the child gives
            // its stderr ("destination already exists", …), which says far
            // more than the resulting broken pipe.
            if let Err(pipe) = stdin.write_all(&data).await {
                drop(stdin);
                return match proc.finish().await {
                    Err(zfs_err) => Err(anyhow::Error::new(zfs_err).context("zfs receive failed")),
                    Ok(()) => Err(anyhow::Error::new(pipe).context("writing to zfs receive")),
                };
            }
            *total += data.len() as u64;
        }
        stdin.flush().await?;
        drop(stdin);
        proc.finish().await?;
    }
    Ok(())
}

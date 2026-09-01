//! `receive`: restore a snapshot and the chain it depends on.
//!
//! Streams are applied oldest-first (the full, then each incremental) with
//! one `zfs receive` per stream. Chunks are fetched ahead of the writer and
//! BLAKE3-verified before a byte reaches zfs; the prefetch depth adapts to
//! whichever side is the bottleneck (see [`apply`]).

use std::collections::VecDeque;

use anyhow::{Context, bail};
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;

use crate::manifest::{Manifest, keys};
use crate::zfs::Zfs;

use super::{Conn, target};

/// Prefetch never buffers more than this, whatever `--window` and the chunk
/// size are, so a large chunk size can't blow memory during restore.
const PREFETCH_BUDGET: u64 = 512 << 20;
/// Always keep at least this many fetches in flight so the writer never
/// blocks on a single round trip.
const MIN_INFLIGHT: usize = 2;

/// Upper bound on concurrent fetches: `--window`, but never more than the
/// byte budget allows for this stream's chunk size.
fn prefetch_ceiling(window: usize, chunk_bytes: u64) -> usize {
    let by_bytes = match PREFETCH_BUDGET.checked_div(chunk_bytes) {
        Some(n) => n.max(1) as usize,
        None => window, // chunk_bytes == 0: size unknown, fall back to the window
    };
    window
        .max(1)
        .min(by_bytes)
        .max(MIN_INFLIGHT.min(window.max(1)))
}

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
    // The chunk size is the sender's, not a receive-side flag, so report the
    // depth prefetch can actually reach for it.
    let chunk = chain
        .iter()
        .flat_map(|m| m.chunks.first())
        .map(|c| c.bytes)
        .max()
        .unwrap_or(0);
    println!(
        "restoring {} stream(s) into {to} ({} bytes total; {} chunks of up to {:.0} MiB, prefetch {}–{})",
        chain.len(),
        chain.iter().map(|m| m.bytes).sum::<u64>(),
        chain.iter().map(|m| m.chunks.len()).sum::<usize>(),
        chunk as f64 / 1_048_576.0,
        MIN_INFLIGHT.min(prefetch_ceiling(window, chunk)),
        prefetch_ceiling(window, chunk),
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

/// Stream one archived snapshot into `zfs receive`, fetching chunks ahead of
/// the writer with an **adaptive** prefetch depth.
///
/// Chunks are fetched in order by spawned tasks; `target` is how many we try
/// to keep in flight. When the next chunk the writer needs is not yet ready —
/// downloads are the bottleneck — `target` grows toward the ceiling; when it
/// is already waiting — ZFS is the bottleneck — `target` eases back down. So
/// a fast link fills up to `prefetch_ceiling` on its own, while a slow writer
/// (or slow pool) settles near `MIN_INFLIGHT` and doesn't buffer memory it
/// can't use. Order is preserved: the writer always consumes chunk N before
/// N+1, whatever finished first.
async fn apply(
    t: &super::Target,
    zfs: &Zfs,
    m: &Manifest,
    to: &str,
    force: bool,
    window: usize,
    total: &mut u64,
) -> anyhow::Result<()> {
    let chunk_bytes = m.chunks.first().map(|c| c.bytes).unwrap_or(0);
    let ceiling = prefetch_ceiling(window, chunk_bytes);
    let floor = MIN_INFLIGHT.min(ceiling);

    let mut proc = zfs.receive(to, force).await?;
    let mut stdin = proc.stdin.take().context("zfs receive has no stdin")?;

    let fetch = |seq: u32, want: String, bytes: u64| -> JoinHandle<anyhow::Result<Bytes>> {
        let key = keys::chunk(&t.prefix, m.dataset_guid, m.snapshot_guid, seq);
        let store = t.store.clone();
        tokio::spawn(async move {
            // Retried: a transient 5xx partway through a long restore would
            // otherwise discard everything applied so far.
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
            Ok(data)
        })
    };

    let mut target = floor;
    let mut next = 0usize; // next chunk index to spawn
    let mut inflight: VecDeque<JoinHandle<anyhow::Result<Bytes>>> = VecDeque::new();

    let spawn_up_to = |inflight: &mut VecDeque<_>, target: usize, next: &mut usize| {
        while inflight.len() < target && *next < m.chunks.len() {
            let c = &m.chunks[*next];
            inflight.push_back(fetch(c.seq, c.blake3.clone(), c.bytes));
            *next += 1;
        }
    };
    spawn_up_to(&mut inflight, target, &mut next);

    while let Some(front) = inflight.pop_front() {
        // Was the chunk the writer needs next already done, or did we have to
        // wait for it? That is the signal for which side is the bottleneck.
        let was_ready = front.is_finished();
        let data = front
            .await
            .map_err(|e| anyhow::anyhow!("fetch task panicked: {e}"))??;
        let prev = target;
        if was_ready {
            target = target.saturating_sub(1).max(floor);
        } else {
            target = (target + 1).min(ceiling);
        }
        if target != prev {
            // Visible with RUST_LOG=zfsbackup_rs=debug: watch the depth track
            // the bottleneck.
            tracing::debug!(
                from = prev,
                to = target,
                bound = if was_ready { "writer" } else { "download" },
                "prefetch depth"
            );
        }

        if let Err(pipe) = stdin.write_all(&data).await {
            // A write failure means zfs exited; collecting the child gives its
            // stderr ("destination already exists", …), which says far more
            // than the resulting broken pipe.
            drop(stdin);
            for h in inflight {
                h.abort();
            }
            return match proc.finish().await {
                Err(zfs_err) => Err(anyhow::Error::new(zfs_err).context("zfs receive failed")),
                Ok(()) => Err(anyhow::Error::new(pipe).context("writing to zfs receive")),
            };
        }
        *total += data.len() as u64;
        spawn_up_to(&mut inflight, target, &mut next);
    }

    stdin.flush().await?;
    drop(stdin);
    proc.finish().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MIN_INFLIGHT, PREFETCH_BUDGET, prefetch_ceiling};

    #[test]
    fn ceiling_respects_window_and_byte_budget() {
        // Small chunks: the window is the limit.
        assert_eq!(prefetch_ceiling(8, 8 << 20), 8);
        // Large chunks: the byte budget caps it below the window.
        assert_eq!(
            prefetch_ceiling(16, 256 << 20),
            (PREFETCH_BUDGET / (256 << 20)) as usize
        );
        // Never below the floor, even with a huge chunk and window 1.
        assert!(prefetch_ceiling(1, 1 << 30) >= 1);
        assert_eq!(prefetch_ceiling(4, 0), 4); // unknown chunk size → window
        assert!(prefetch_ceiling(16, 64 << 20) >= MIN_INFLIGHT);
    }
}

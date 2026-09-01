//! `send`: archive one snapshot.
//!
//! The stream is read once and verified as it flows: the parser checks every
//! ZFS record's fletcher4 checksum, BLAKE3 is computed per chunk and for the
//! whole stream, and each chunk is uploaded with a server-verified CRC32C.
//! The manifest is written last — its presence is the commit point, so an
//! interrupted run leaves only resumable chunks, never a half-visible backup.

use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use anyhow::{Context, bail};
use bytes::Bytes;
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;

use crate::manifest::{Chunk, FORMAT_VERSION, Manifest, Pending, keys};
use crate::stream::{Event, StreamParser};
use crate::types::{Guid, SendFlags};
use crate::zfs::{SendSpec, Zfs, tags};

use super::{Conn, Target, retry, target};

pub struct Args {
    pub snapshot: String,
    pub uri: String,
    pub from: Option<String>,
    pub full: bool,
    pub chunk_size: u64,
    /// Choose the chunk size from the estimated stream size instead of using
    /// `chunk_size`.
    pub adaptive_chunk_size: bool,
    /// Lower / upper bound for the adaptive size.
    pub adaptive_chunk_min: u64,
    pub adaptive_chunk_max: u64,
    pub parallel: usize,
    pub conn: Conn,
    pub zfs_bin: String,
}

const JOB: &str = "default";
const MIN_CHUNK: u64 = 5 << 20;
/// S3 rejects a single PutObject above 5 GiB, and the chunk is held in
/// memory before it goes out.
const MAX_CHUNK: u64 = 5 << 30;
/// How often the in-progress marker's lease is renewed while uploading.
const LEASE_REFRESH: std::time::Duration = std::time::Duration::from_secs(60);

/// Roughly how many chunks `--adaptive-chunk-size` aims for. Enough to keep
/// uploads parallel and resume granular, few enough to keep the object count
/// and manifest small.
const ADAPTIVE_TARGET_CHUNKS: u64 = 1_000;

/// Chunk size for a stream of `total` bytes: aim for ADAPTIVE_TARGET_CHUNKS,
/// rounded up to a 16 MiB boundary, clamped to `[min, max]`.
fn adaptive_chunk_size(total: u64, min: u64, max: u64) -> u64 {
    let unit = 16 << 20;
    let raw = (total / ADAPTIVE_TARGET_CHUNKS).clamp(min, max);
    raw.div_ceil(unit).saturating_mul(unit).clamp(min, max)
}

pub async fn run(mut a: Args) -> anyhow::Result<()> {
    if a.chunk_size < MIN_CHUNK {
        bail!("chunk size must be at least 5MiB");
    }
    if a.chunk_size > MAX_CHUNK {
        bail!("chunk size must be at most 5GiB (a chunk is one PutObject, held in memory)");
    }
    if a.adaptive_chunk_size {
        if a.adaptive_chunk_min < MIN_CHUNK || a.adaptive_chunk_max > MAX_CHUNK {
            bail!("adaptive chunk bounds must be within 5MiB–5GiB");
        }
        if a.adaptive_chunk_min > a.adaptive_chunk_max {
            bail!("--adaptive-chunk-min must not exceed --adaptive-chunk-max");
        }
    }
    let t = target(&a.uri, &a.conn)?;
    let zfs = Zfs::new().with_binary(&a.zfs_bin);
    let snap = zfs.snapshot(&a.snapshot).await?;
    let ds = zfs.dataset(&snap.dataset).await?;

    let manifests = t.manifests().await?;
    if let Some(m) = manifests.iter().find(|m| m.snapshot_guid == snap.guid) {
        println!("{} is already archived ({} bytes)", m.snapshot, m.bytes);
        // The snapshot is safely stored, so any hold this tool left behind
        // (an interrupted earlier run) has done its job and is dropped here;
        // otherwise it would keep blocking `zfs destroy` on that snapshot.
        if let Err(e) = zfs.release(&tags::hold(JOB), &a.snapshot).await {
            tracing::warn!(error = %e, "could not release hold {}", tags::hold(JOB));
        }
        return Ok(());
    }

    // Base selection: explicit --from, else the newest archived snapshot of
    // this dataset that still exists locally as a snapshot or zb bookmark.
    let (from_name, from_guid) = if a.full {
        (None, None)
    } else if let Some(f) = &a.from {
        let g = resolve_base_guid(&zfs, &snap.dataset, f)
            .await
            .with_context(|| format!("--from {f}"))?;
        if !manifests
            .iter()
            .any(|m| m.dataset_guid == ds.guid && m.snapshot_guid == g)
        {
            bail!(
                "--from {f}: that snapshot is not archived in this bucket; restores need the whole chain"
            );
        }
        (Some(f.clone()), Some(g))
    } else {
        auto_base(&zfs, &manifests, ds.guid, &snap.dataset, snap.createtxg).await?
    };

    let flags = SendFlags {
        raw: ds.encrypted,
        compressed: true,
        large_blocks: true,
        ..Default::default()
    };
    // `zfs send -nP` says how big the stream will be, so the run reports a
    // size up front instead of only in hindsight.
    let spec = SendSpec {
        to: a.snapshot.clone(),
        from: from_name.clone(),
        flags,
    };
    let estimate = match zfs.estimate(&spec).await {
        Ok(n) => Some(n),
        Err(e) => {
            tracing::debug!(error = %e, "could not estimate the stream size");
            None
        }
    };
    let size = estimate
        .map(|n| format!(" (~{:.1} GiB)", n as f64 / 1_073_741_824.0))
        .unwrap_or_default();
    match &from_name {
        Some(f) => println!("incremental {} from {f}{size}", a.snapshot),
        None => println!("full {}{size}", a.snapshot),
    }

    // Size the chunk to the job when asked: too-small chunks on a huge stream
    // mean needless round trips, too-large on a small one waste memory and
    // parallelism. Needs the estimate; without it, keep the given size.
    if a.adaptive_chunk_size {
        match estimate {
            Some(n) => {
                a.chunk_size = adaptive_chunk_size(n, a.adaptive_chunk_min, a.adaptive_chunk_max);
                println!(
                    "adaptive chunk size: {} MiB (~{} chunks, bounds {}–{} MiB)",
                    a.chunk_size / (1 << 20),
                    n.div_ceil(a.chunk_size),
                    a.adaptive_chunk_min / (1 << 20),
                    a.adaptive_chunk_max / (1 << 20),
                );
            }
            None => println!(
                "adaptive chunk size: no estimate available, using {} MiB",
                a.chunk_size / (1 << 20)
            ),
        }
    }

    // Hold the snapshot while it is being read. The tag belongs to this tool,
    // so it is released below whether or not this run placed it: an earlier
    // interrupted run leaves its hold behind, and skipping the release would
    // strand it — blocking `zfs destroy` on that snapshot forever.
    zfs.hold(&tags::hold(JOB), &a.snapshot).await?;

    // A signal ends the upload through the same path as an error, so the
    // hold below is always released.
    let result = tokio::select! {
        r = send_stream(&t, &zfs, &a, StreamSpec {
            ds_guid: ds.guid,
            snap_guid: snap.guid,
            from_name: from_name.clone(),
            from_guid,
            flags,
        }) => r,
        how = interrupted() => Err(anyhow::anyhow!("{how}; the partial upload can be resumed by re-running")),
    };

    // The hold's job is done once the stream is fully uploaded (or failed).
    if let Err(e) = zfs.release(&tags::hold(JOB), &a.snapshot).await {
        tracing::warn!(error = %e, "could not release hold {}; `zfs release {} {}` clears it", tags::hold(JOB), tags::hold(JOB), a.snapshot);
    }
    let (chunks, bytes, stream_blake3, end_checksum, secs) = result?;

    let m = Manifest {
        format_version: FORMAT_VERSION,
        dataset: snap.dataset.clone(),
        dataset_guid: ds.guid,
        snapshot: a.snapshot.clone(),
        snapshot_guid: snap.guid,
        from_guid,
        createtxg: snap.createtxg,
        created_at: time::OffsetDateTime::now_utc(),
        send_flags: flags,
        bytes,
        stream_blake3,
        end_checksum,
        chunks,
    };
    let key = keys::manifest(&t.prefix, ds.guid, snap.guid);
    let body = Bytes::from(m.encode()?);
    retry("write manifest", || {
        let b = body.clone();
        let key = key.clone();
        let t = &t;
        async move { Ok(t.store.put(&key, b).await?) }
    })
    .await?;

    // The send is committed; the resume marker has served its purpose.
    if let Err(e) = t
        .store
        .delete(&keys::pending(&t.prefix, ds.guid, snap.guid))
        .await
    {
        tracing::warn!(error = %e, "could not remove the resume marker (harmless; the next send overwrites it)");
    }

    // A bookmark survives snapshot destruction, keeping future incrementals
    // possible; one zb bookmark per dataset, at the newest archived snapshot.
    // Older bookmarks are pruned only once the new one exists, so a failed
    // bookmark leaves the dataset with a usable base.
    let bm = tags::bookmark(JOB, &a.snapshot, snap.guid);
    match zfs.bookmark(&a.snapshot, &bm).await {
        Ok(()) => match zfs.bookmarks(&snap.dataset).await {
            Ok(marks) => {
                for b in marks {
                    if let Some(g) = tags::bookmark_guid(JOB, &b.name)
                        && g != snap.guid
                        && let Err(e) = zfs.destroy_bookmark(&b.name).await
                    {
                        tracing::warn!(error = %e, "could not remove old bookmark {}", b.name);
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "could not list bookmarks for cleanup"),
        },
        Err(e) => tracing::warn!(
            error = %e,
            "could not create bookmark {bm}; keeping the previous one so incrementals still have a base"
        ),
    }

    let mbs = bytes as f64 / secs / 1e6;
    println!(
        "archived {} — {} bytes in {} chunks, {:.1}s ({:.1} MB/s)",
        a.snapshot,
        bytes,
        m.chunks.len(),
        secs,
        mbs
    );
    Ok(())
}

/// Read the stream, upload chunks (resuming past ones already present), and
/// return (chunks, bytes, stream blake3, end checksum, seconds).
/// What identifies the stream being produced, as opposed to how it is
/// uploaded (which lives in `Args`).
struct StreamSpec {
    ds_guid: Guid,
    snap_guid: Guid,
    from_name: Option<String>,
    from_guid: Option<Guid>,
    flags: SendFlags,
}

async fn send_stream(
    t: &Target,
    zfs: &Zfs,
    a: &Args,
    spec: StreamSpec,
) -> anyhow::Result<(Vec<Chunk>, u64, String, Option<String>, f64)> {
    let StreamSpec {
        ds_guid,
        snap_guid,
        from_name,
        from_guid,
        flags,
    } = spec;
    // ds_guid comes from run()'s earlier `zfs.dataset` call — no second lookup.
    let dir = format!("{}/", keys::snapshot_dir(&t.prefix, ds_guid, snap_guid));
    let mut existing: BTreeMap<String, u64> = t.store.list(&dir).await?.into_iter().collect();

    // Chunk keys depend only on the snapshot, while chunk contents depend on
    // the base and the send flags. Resume only when the interrupted run was
    // producing this same stream; a different base would leave unrelated
    // bytes under the right names, so those chunks are cleared instead.
    let marker_key = keys::pending(&t.prefix, ds_guid, snap_guid);
    let mut mine = Pending {
        from_guid,
        send_flags: flags,
        chunk_size: a.chunk_size,
        run_id: uuid::Uuid::now_v7(),
        refreshed_at: time::OffsetDateTime::now_utc(),
    };
    let previous = match t.store.get(&marker_key).await {
        Ok(b) => Some(b),
        Err(crate::store::StoreError::NotFound(_)) => None,
        Err(e) => return Err(e.into()),
    };
    let resume = match previous.as_deref().map(Pending::decode) {
        Some(Ok(p)) => {
            // A marker whose lease is still live belongs to a run that is
            // very likely still uploading. Two runs sharing these keys can
            // commit a manifest describing the other's bytes, so this one
            // steps aside instead of touching anything.
            if p.lease_live(time::OffsetDateTime::now_utc()) && p.run_id != mine.run_id {
                bail!(
                    "another send of {} is in progress (run {}, last seen {}); \
                     wait for it, or delete {marker_key} if you know it is dead",
                    a.snapshot,
                    p.run_id,
                    p.refreshed_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_else(|_| p.refreshed_at.to_string())
                );
            }
            if p.same_stream(&mine) {
                true
            } else {
                println!(
                    "previous attempt used different send parameters; starting this one clean"
                );
                false
            }
        }
        Some(Err(e)) => {
            tracing::warn!(error = %e, "unreadable resume marker; starting clean");
            false
        }
        None => false,
    };
    if !resume && !existing.is_empty() {
        let (objects, uploads) = t.store.purge_prefix(&dir).await?;
        tracing::info!(objects, uploads, "cleared chunks from an unrelated attempt");
        existing.clear();
    }
    write_marker(t, &marker_key, &mine).await?;
    let mut refreshed = Instant::now();

    let mut proc = zfs
        .send(&SendSpec {
            to: a.snapshot.clone(),
            from: from_name,
            flags,
        })
        .await?;
    let mut stdout = proc.take_stdout();

    let started = Instant::now();
    let mut parser = StreamParser::new();
    let mut end_checksum: Option<String> = None;
    let mut stream_hash = blake3::Hasher::new();
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut inflight: JoinSet<anyhow::Result<u32>> = JoinSet::new();
    let mut uploaded: HashSet<u32> = HashSet::new();
    let mut seq: u32 = 0;
    let mut total: u64 = 0;
    let chunk_size = a.chunk_size as usize;

    loop {
        // Fill one chunk from the pipe, reading straight into buf's reserved
        // spare capacity — no per-read scratch buffer, no zero-fill, no second
        // copy. On a multi-TB stream that removes a whole extra pass over every
        // byte. read_buf writes into the uninitialized tail (Vec<u8>: BufMut)
        // and grows the length by what it read.
        let mut buf: Vec<u8> = Vec::with_capacity(chunk_size);
        while buf.len() < chunk_size {
            if stdout.read_buf(&mut buf).await? == 0 {
                break;
            }
        }
        if buf.is_empty() {
            break;
        }
        for ev in parser.feed(&buf)? {
            if let Event::End(e) = ev {
                end_checksum = Some(hex::encode(e.checksum.to_bytes()));
            }
        }
        // Keep the lease alive so a concurrent run can tell this one apart
        // from a crashed one.
        if refreshed.elapsed() >= LEASE_REFRESH {
            mine.refreshed_at = time::OffsetDateTime::now_utc();
            write_marker(t, &marker_key, &mine).await?;
            refreshed = Instant::now();
        }
        stream_hash.update_rayon(&buf);
        let blake3 = blake3::hash(&buf).to_hex().to_string();
        // CRC32C over the chunk, computed once here and passed to the upload as
        // the `x-amz-checksum-crc32c` write-time integrity header. Memory-speed.
        let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, &buf) as u32;
        let bytes = buf.len() as u64;
        total += bytes;
        let key = keys::chunk(&t.prefix, ds_guid, snap_guid, seq);
        chunks.push(Chunk { seq, bytes, blake3 });

        if existing.get(&key) == Some(&bytes) {
            tracing::info!(seq, "chunk already uploaded; skipped");
            uploaded.insert(seq);
        } else {
            // Make room *before* handing off, so the next chunk is read from
            // the pipe while a full `parallel` uploads are in flight. Draining
            // afterwards left one slot idle for the whole read.
            while inflight.len() >= a.parallel.max(1) {
                let Some(r) = inflight.join_next().await else {
                    break;
                };
                uploaded.insert(r??);
            }
            let store = t.store.clone();
            let data = Bytes::from(buf);
            let this = seq;
            inflight.spawn(async move {
                retry("upload chunk", || {
                    let store = store.clone();
                    let key = key.clone();
                    let data = data.clone();
                    async move { Ok(store.put_verified(&key, data, crc).await?) }
                })
                .await?;
                Ok(this)
            });
        }
        seq += 1;
    }
    while let Some(r) = inflight.join_next().await {
        uploaded.insert(r??);
    }
    // Collect the child first: when `zfs send` dies mid-stream its stderr
    // says why (faulted pool, destroyed base), which is more useful than the
    // truncation the parser would report from the same event.
    proc.wait().await?;
    parser.finish()?;
    if uploaded.len() != chunks.len() {
        bail!(
            "uploaded {} of {} chunks — refusing to write the manifest",
            uploaded.len(),
            chunks.len()
        );
    }
    Ok((
        chunks,
        total,
        stream_hash.finalize().to_hex().to_string(),
        end_checksum,
        started.elapsed().as_secs_f64(),
    ))
}

async fn write_marker(t: &Target, key: &str, p: &Pending) -> anyhow::Result<()> {
    let body = Bytes::from(p.encode()?);
    retry("write resume marker", || {
        let body = body.clone();
        let key = key.to_string();
        async move { Ok(t.store.put(&key, body).await?) }
    })
    .await
}

/// Resolves when the process is asked to stop, so the snapshot's hold can be
/// released before exiting. Without this a Ctrl-C leaves `zb:default` on the
/// snapshot and every later `zfs destroy` of it fails with "dataset is busy".
async fn interrupted() -> &'static str {
    #[cfg(unix)]
    {
        let mut term = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "no SIGTERM handler; only Ctrl-C will release the hold");
                let _ = tokio::signal::ctrl_c().await;
                return "interrupted";
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => "interrupted",
            _ = term.recv() => "terminated",
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "interrupted"
    }
}

/// The GUID of an explicit `--from` (snapshot `@s` / `pool/ds@s`, or
/// bookmark `#b` / `pool/ds#b`).
async fn resolve_base_guid(zfs: &Zfs, dataset: &str, from: &str) -> anyhow::Result<Guid> {
    let full = if from.starts_with('@') || from.starts_with('#') {
        format!("{dataset}{from}")
    } else {
        from.to_string()
    };
    if full.contains('#') {
        let marks = zfs.bookmarks(dataset).await?;
        return marks
            .into_iter()
            .find(|b| b.name == full)
            .map(|b| b.guid)
            .with_context(|| format!("bookmark {full} not found"));
    }
    Ok(zfs.snapshot(&full).await?.guid)
}

/// Most recently archived snapshot of this dataset that still exists locally
/// as a snapshot or zb bookmark and precedes the one being sent.
///
/// Candidates are ordered by when they were archived, which stays comparable
/// across pools; `createtxg` orders snapshots only within the pool that
/// produced them, so a dataset restored onto a new pool mixes two scales.
/// Local txgs still gate a candidate against the target, and zfs rejects any
/// base that is not an ancestor.
async fn auto_base(
    zfs: &Zfs,
    manifests: &[Manifest],
    ds_guid: Guid,
    dataset: &str,
    to_txg: u64,
) -> anyhow::Result<(Option<String>, Option<Guid>)> {
    let local_snaps = zfs.snapshots(dataset).await?;
    let local_marks = zfs.bookmarks(dataset).await?;
    let mut candidates: Vec<&Manifest> = manifests
        .iter()
        .filter(|m| m.dataset_guid == ds_guid)
        .collect();
    candidates.sort_by_key(|m| std::cmp::Reverse(m.created_at));
    for m in candidates {
        if let Some(s) = local_snaps
            .iter()
            .find(|s| s.guid == m.snapshot_guid && s.createtxg < to_txg)
        {
            return Ok((Some(s.name.clone()), Some(m.snapshot_guid)));
        }
        if let Some(b) = local_marks
            .iter()
            .find(|b| b.guid == m.snapshot_guid && b.createtxg < to_txg)
        {
            return Ok((Some(b.name.clone()), Some(m.snapshot_guid)));
        }
    }
    println!("no archived base found locally; sending a full stream");
    Ok((None, None))
}

#[cfg(test)]
mod tests {
    use super::adaptive_chunk_size;

    #[test]
    fn adaptive_targets_a_sane_chunk_count() {
        let gib = 1u64 << 30;
        let mib = 1u64 << 20;
        let (min, max) = (16 * mib, 512 * mib);

        // Tiny stream clamps up to the floor; huge clamps to the ceiling.
        assert_eq!(adaptive_chunk_size(100 * mib, min, max), min);
        assert_eq!(adaptive_chunk_size(10 * 1024 * gib, min, max), max);

        // Mid stream lands in range, on a 16 MiB boundary, near the target.
        let c = adaptive_chunk_size(400 * gib, min, max);
        assert!((min..=max).contains(&c));
        assert_eq!(c % (16 * mib), 0);
        let chunks = (400 * gib).div_ceil(c);
        assert!((500..=2000).contains(&chunks), "got {chunks} chunks");
    }

    #[test]
    fn adaptive_respects_custom_bounds() {
        let gib = 1u64 << 30;
        let tib = 1024 * gib;
        // Hundreds of TB with a raised ceiling: chunks scale up to the max.
        assert_eq!(adaptive_chunk_size(300 * tib, 64 << 20, 2 * gib), 2 * gib);
        // A raised floor is honoured for a modest stream.
        assert_eq!(adaptive_chunk_size(gib, 128 << 20, gib), 128 << 20);
    }
}

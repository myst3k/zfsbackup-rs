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

use super::{Target, retry, target};

pub struct Args {
    pub snapshot: String,
    pub uri: String,
    pub from: Option<String>,
    pub full: bool,
    pub chunk_size: u64,
    pub parallel: usize,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub zfs_bin: String,
}

const JOB: &str = "default";
const MIN_CHUNK: u64 = 5 << 20;

pub async fn run(a: Args) -> anyhow::Result<()> {
    if a.chunk_size < MIN_CHUNK {
        bail!("chunk size must be at least 5MiB");
    }
    let t = target(&a.uri, a.endpoint.as_deref(), a.region.as_deref())?;
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
    match &from_name {
        Some(f) => println!("incremental {} from {f}", a.snapshot),
        None => println!("full {}", a.snapshot),
    }

    // Hold the snapshot while it is being read. The tag belongs to this tool,
    // so it is released below whether or not this run placed it: an earlier
    // interrupted run leaves its hold behind, and skipping the release would
    // strand it — blocking `zfs destroy` on that snapshot forever.
    zfs.hold(&tags::hold(JOB), &a.snapshot).await?;

    let result = send_stream(
        &t,
        &zfs,
        &a,
        &snap.dataset,
        snap.guid,
        from_name.clone(),
        from_guid,
        flags,
    )
    .await;

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
async fn send_stream(
    t: &Target,
    zfs: &Zfs,
    a: &Args,
    dataset: &str,
    snap_guid: Guid,
    from_name: Option<String>,
    from_guid: Option<Guid>,
    flags: SendFlags,
) -> anyhow::Result<(Vec<Chunk>, u64, String, Option<String>, f64)> {
    let ds_guid = zfs.dataset(dataset).await?.guid;
    let dir = format!("{}/", keys::snapshot_dir(&t.prefix, ds_guid, snap_guid));
    let mut existing: BTreeMap<String, u64> = t.store.list(&dir).await?.into_iter().collect();

    // Chunk keys depend only on the snapshot, while chunk contents depend on
    // the base and the send flags. Resume only when the interrupted run was
    // producing this same stream; a different base would leave unrelated
    // bytes under the right names, so those chunks are cleared instead.
    let marker_key = keys::pending(&t.prefix, ds_guid, snap_guid);
    let want = Pending {
        from_guid,
        send_flags: flags,
        chunk_size: a.chunk_size,
    };
    let previous = match t.store.get(&marker_key).await {
        Ok(b) => Some(b),
        Err(crate::store::StoreError::NotFound(_)) => None,
        Err(e) => return Err(e.into()),
    };
    let resume = match previous.as_deref().map(Pending::decode) {
        Some(Ok(p)) if p == want => true,
        Some(Ok(_)) => {
            println!("previous attempt used different send parameters; starting this one clean");
            false
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
    let marker = Bytes::from(want.encode()?);
    retry("write resume marker", || {
        let body = marker.clone();
        let key = marker_key.clone();
        async move { Ok(t.store.put(&key, body).await?) }
    })
    .await?;

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
        // Fill one chunk from the pipe.
        let mut buf = Vec::with_capacity(chunk_size);
        while buf.len() < chunk_size {
            let mut piece = vec![0u8; (chunk_size - buf.len()).min(1 << 20)];
            let n = stdout.read(&mut piece).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&piece[..n]);
        }
        if buf.is_empty() {
            break;
        }
        for ev in parser.feed(&buf)? {
            if let Event::End(e) = ev {
                end_checksum = Some(hex::encode(e.checksum.to_bytes()));
            }
        }
        stream_hash.update_rayon(&buf);
        let blake3 = blake3::hash(&buf).to_hex().to_string();
        let bytes = buf.len() as u64;
        total += bytes;
        let key = keys::chunk(&t.prefix, ds_guid, snap_guid, seq);
        chunks.push(Chunk { seq, bytes, blake3 });

        if existing.get(&key) == Some(&bytes) {
            tracing::info!(seq, "chunk already uploaded; skipped");
            uploaded.insert(seq);
        } else {
            let store = t.store.clone();
            let data = Bytes::from(buf);
            let this = seq;
            inflight.spawn(async move {
                retry("upload chunk", || {
                    let store = store.clone();
                    let key = key.clone();
                    let data = data.clone();
                    async move { Ok(store.put_verified(&key, data).await?) }
                })
                .await?;
                Ok(this)
            });
            while inflight.len() >= a.parallel.max(1) {
                let Some(r) = inflight.join_next().await else {
                    break;
                };
                uploaded.insert(r??);
            }
        }
        seq += 1;
    }
    while let Some(r) = inflight.join_next().await {
        uploaded.insert(r??);
    }
    parser.finish()?;
    proc.wait().await?;
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

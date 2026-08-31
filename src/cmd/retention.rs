//! `retention`: expire by age and count, never breaking a chain.
//!
//! A snapshot is deleted only when (a) it is older than `--older-than`,
//! (b) it is not among the newest `--keep-last` of its dataset, (c) it is
//! not pinned, and (d) no snapshot that is being kept depends on it —
//! directly or transitively. The manifest is deleted before the chunks, so
//! an interrupted run leaves garbage (invisible, re-collectable) rather than
//! a listed backup with missing data.

use std::collections::{HashMap, HashSet};

use anyhow::bail;
use time::{Duration, OffsetDateTime};

use crate::manifest::{Manifest, keys};
use crate::types::Guid;

use super::target;

pub async fn run(
    uri: &str,
    older_than: Option<&str>,
    keep_last: usize,
    dry_run: bool,
    endpoint: Option<&str>,
    region: Option<&str>,
) -> anyhow::Result<()> {
    let t = target(uri, endpoint, region)?;
    let all = t.manifests().await?;
    let pinned: HashSet<Guid> = t.pinned().await?.into_iter().collect();
    let cutoff = match older_than {
        Some(s) => Some(OffsetDateTime::now_utc() - parse_age(s)?),
        None => None,
    };

    let by_guid: HashMap<Guid, &Manifest> = all.iter().map(|m| (m.snapshot_guid, m)).collect();
    let mut keep: HashSet<Guid> = HashSet::new();

    // Newest keep_last per dataset, everything newer than the cutoff, pins.
    let mut per_dataset: HashMap<Guid, Vec<&Manifest>> = HashMap::new();
    for m in &all {
        per_dataset.entry(m.dataset_guid).or_default().push(m);
    }
    for list in per_dataset.values_mut() {
        list.sort_by_key(|m| std::cmp::Reverse(m.createtxg));
        for m in list.iter().take(keep_last.max(1)) {
            keep.insert(m.snapshot_guid);
        }
    }
    for m in &all {
        let too_young = cutoff.is_none_or(|c| m.created_at > c);
        if too_young || pinned.contains(&m.snapshot_guid) {
            keep.insert(m.snapshot_guid);
        }
    }
    // A kept incremental keeps its whole ancestry.
    loop {
        let mut added = false;
        for g in keep.clone() {
            if let Some(m) = by_guid.get(&g)
                && let Some(from) = m.from_guid
                && by_guid.contains_key(&from)
                && keep.insert(from)
            {
                added = true;
            }
        }
        if !added {
            break;
        }
    }

    let doomed: Vec<&Manifest> = all
        .iter()
        .filter(|m| !keep.contains(&m.snapshot_guid))
        .collect();
    if doomed.is_empty() {
        println!("nothing to delete ({} snapshot(s) kept)", all.len());
        return Ok(());
    }
    let mut freed = 0u64;
    for m in &doomed {
        freed += m.bytes;
        println!(
            "{} {} ({} bytes, {})",
            if dry_run { "would delete" } else { "deleting" },
            m.snapshot,
            m.bytes,
            if m.is_full() { "full" } else { "incremental" }
        );
        if dry_run {
            continue;
        }
        // Manifest first: from this moment the backup is invisible.
        t.store
            .delete(&keys::manifest(&t.prefix, m.dataset_guid, m.snapshot_guid))
            .await?;
        let dir = format!(
            "{}/",
            keys::snapshot_dir(&t.prefix, m.dataset_guid, m.snapshot_guid)
        );
        let (objects, uploads) = t.store.purge_prefix(&dir).await?;
        tracing::info!(snapshot = m.snapshot, objects, uploads, "chunks removed");
    }
    println!(
        "{}: {} snapshot(s), {} bytes{}",
        if dry_run { "dry run" } else { "deleted" },
        doomed.len(),
        freed,
        if dry_run { " (nothing removed)" } else { "" }
    );
    Ok(())
}

/// "90d", "12w", "24h", "30m".
fn parse_age(s: &str) -> anyhow::Result<Duration> {
    let t = s.trim();
    let (num, unit) = t.split_at(t.len().saturating_sub(1));
    let n: i64 = num
        .parse()
        .map_err(|e| anyhow::anyhow!("{s:?}: expected e.g. 90d, 12w, 24h: {e}"))?;
    Ok(match unit {
        "d" => Duration::days(n),
        "w" => Duration::weeks(n),
        "h" => Duration::hours(n),
        "m" => Duration::minutes(n),
        _ => bail!("{s:?}: unit must be one of d, w, h, m"),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_age;

    #[test]
    fn ages() {
        assert_eq!(parse_age("90d").unwrap(), time::Duration::days(90));
        assert_eq!(parse_age("24h").unwrap(), time::Duration::hours(24));
        assert!(parse_age("90x").is_err());
        assert!(parse_age("d").is_err());
    }
}

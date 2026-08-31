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
    keep_last: Option<usize>,
    dry_run: bool,
    endpoint: Option<&str>,
    region: Option<&str>,
) -> anyhow::Result<()> {
    // Deleting backups is explicit work: one of the two policy flags must
    // say what to keep, so a bare `retention <uri>` can never delete.
    if older_than.is_none() && keep_last.is_none() {
        bail!("give a policy: --older-than (e.g. 90d), --keep-last N, or both");
    }
    let keep_last = keep_last.unwrap_or(1).max(1);
    let t = target(uri, endpoint, region)?;
    let all = t.manifests().await?;
    let pinned: HashSet<Guid> = t.pinned().await?.into_iter().collect();
    let cutoff = match older_than {
        Some(s) => Some(OffsetDateTime::now_utc() - parse_age(s)?),
        None => None,
    };
    println!(
        "policy: keep the newest {keep_last} per dataset{}",
        match older_than {
            Some(s) => format!(", plus anything newer than {s}"),
            None => String::new(),
        }
    );

    let keep = keep_set(&all, &pinned, cutoff, keep_last);
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

/// Which snapshots survive: the newest `keep_last` of each dataset, anything
/// archived after `cutoff`, every pin — and the full ancestry of all of those,
/// so a surviving incremental always has the chain it needs to restore.
fn keep_set(
    all: &[Manifest],
    pinned: &HashSet<Guid>,
    cutoff: Option<OffsetDateTime>,
    keep_last: usize,
) -> HashSet<Guid> {
    let by_guid: HashMap<Guid, &Manifest> = all.iter().map(|m| (m.snapshot_guid, m)).collect();
    let mut keep: HashSet<Guid> = HashSet::new();

    let mut per_dataset: HashMap<Guid, Vec<&Manifest>> = HashMap::new();
    for m in all {
        per_dataset.entry(m.dataset_guid).or_default().push(m);
    }
    // Ordered by when each backup was archived: `createtxg` is pool-local, so
    // a dataset restored onto another pool would otherwise rank stale
    // pre-restore backups above current ones.
    for list in per_dataset.values_mut() {
        list.sort_by_key(|m| std::cmp::Reverse(m.created_at));
        for m in list.iter().take(keep_last) {
            keep.insert(m.snapshot_guid);
        }
    }
    for m in all {
        let within_age = cutoff.is_some_and(|c| m.created_at > c);
        if within_age || pinned.contains(&m.snapshot_guid) {
            keep.insert(m.snapshot_guid);
        }
    }
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
    keep
}

/// "90d", "12w", "24h", "30m".
///
/// A century is the ceiling: `Duration::days` itself panics on values near
/// `i64::MAX`, and a mistyped extra digit should be a message, not a crash.
const MAX_AGE_DAYS: i64 = 36_500;

fn parse_age(s: &str) -> anyhow::Result<Duration> {
    let t = s.trim();
    // Split on the last *character*, not the last byte: `90д` would otherwise
    // land inside a multi-byte character and panic.
    let (num, unit) = match t.char_indices().next_back() {
        Some((i, c)) => (&t[..i], c),
        None => bail!("empty age: expected e.g. 90d, 12w, 24h"),
    };
    let n: i64 = num
        .parse()
        .map_err(|e| anyhow::anyhow!("{s:?}: expected e.g. 90d, 12w, 24h: {e}"))?;
    if n < 0 {
        bail!("{s:?}: age cannot be negative");
    }
    let days_equivalent = match unit {
        'd' => n,
        'w' => n.saturating_mul(7),
        'h' => n / 24,
        'm' => n / (24 * 60),
        _ => bail!("{s:?}: unit must be one of d, w, h, m"),
    };
    if days_equivalent > MAX_AGE_DAYS {
        bail!("{s:?}: age is longer than {MAX_AGE_DAYS} days");
    }
    Ok(match unit {
        'd' => Duration::days(n),
        'w' => Duration::weeks(n),
        'h' => Duration::hours(n),
        'm' => Duration::minutes(n),
        _ => unreachable!("unit validated above"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::FORMAT_VERSION;

    #[test]
    fn ages() {
        assert_eq!(parse_age("90d").unwrap(), time::Duration::days(90));
        assert_eq!(parse_age("24h").unwrap(), time::Duration::hours(24));
        assert!(parse_age("90x").is_err());
        assert!(parse_age("d").is_err());
        assert!(parse_age("").is_err());
        // Errors instead of panicking: multi-byte suffix, absurd magnitude,
        // and a negative value all reach the user as messages.
        assert!(parse_age("90д").is_err());
        assert!(parse_age("9999999d").is_err());
        assert!(parse_age("-5d").is_err());
        assert!(parse_age(&format!("{}d", i64::MAX)).is_err());
    }

    /// `ds` dataset, snapshot `guid`, optional base, archived `age_days` ago.
    fn m(ds: u64, guid: u64, from: Option<u64>, age_days: i64) -> Manifest {
        Manifest {
            format_version: FORMAT_VERSION,
            dataset: format!("tank/d{ds}"),
            dataset_guid: Guid(ds),
            snapshot: format!("tank/d{ds}@s{guid}"),
            snapshot_guid: Guid(guid),
            from_guid: from.map(Guid),
            createtxg: guid,
            created_at: OffsetDateTime::now_utc() - Duration::days(age_days),
            send_flags: Default::default(),
            bytes: 1,
            stream_blake3: String::new(),
            end_checksum: None,
            chunks: Vec::new(),
        }
    }

    fn kept(
        all: &[Manifest],
        pins: &[u64],
        cutoff_days: Option<i64>,
        keep_last: usize,
    ) -> Vec<u64> {
        let pinned: HashSet<Guid> = pins.iter().map(|g| Guid(*g)).collect();
        let cutoff = cutoff_days.map(|d| OffsetDateTime::now_utc() - Duration::days(d));
        let mut v: Vec<u64> = keep_set(all, &pinned, cutoff, keep_last)
            .into_iter()
            .map(|g| g.0)
            .collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn keep_last_alone_prunes() {
        // full 1 ← 2 ← 3, all old. Keeping the newest one keeps its ancestry.
        let all = vec![
            m(9, 1, None, 30),
            m(9, 2, Some(1), 20),
            m(9, 3, Some(2), 10),
        ];
        assert_eq!(kept(&all, &[], None, 1), vec![1, 2, 3]);
    }

    #[test]
    fn independent_fulls_are_deleted_beyond_keep_last() {
        let all = vec![m(9, 1, None, 30), m(9, 2, None, 20), m(9, 3, None, 10)];
        assert_eq!(kept(&all, &[], None, 1), vec![3]);
        assert_eq!(kept(&all, &[], None, 2), vec![2, 3]);
    }

    #[test]
    fn age_keeps_recent_and_their_bases() {
        // Only s3 is inside a 15-day window, and it drags 1 and 2 along.
        let all = vec![
            m(9, 1, None, 30),
            m(9, 2, Some(1), 20),
            m(9, 3, Some(2), 10),
        ];
        assert_eq!(kept(&all, &[], Some(15), 0), vec![1, 2, 3]);
        // Independent fulls: the old ones go.
        let all = vec![m(9, 1, None, 30), m(9, 2, None, 20), m(9, 3, None, 10)];
        assert_eq!(kept(&all, &[], Some(15), 0), vec![3]);
    }

    #[test]
    fn pins_survive_with_their_ancestry() {
        let all = vec![m(9, 1, None, 90), m(9, 2, Some(1), 80), m(9, 3, None, 10)];
        assert_eq!(kept(&all, &[2], None, 1), vec![1, 2, 3]);
    }

    #[test]
    fn datasets_are_counted_separately() {
        let all = vec![m(1, 10, None, 5), m(1, 11, None, 4), m(2, 20, None, 3)];
        assert_eq!(kept(&all, &[], None, 1), vec![11, 20]);
    }

    #[test]
    fn newest_is_by_archive_time_not_pool_txg() {
        // Post-restore backup: small createtxg, but archived most recently.
        let mut fresh = m(9, 5, None, 1);
        fresh.createtxg = 100;
        let mut stale = m(9, 6, None, 40);
        stale.createtxg = 9_000_000;
        assert_eq!(kept(&[stale, fresh], &[], None, 1), vec![5]);
    }
}

//! `clean`: remove objects no backup refers to.
//!
//! Two kinds of garbage accumulate in a bucket, and neither is visible to
//! `list` or reachable by `retention`, because both work from manifests:
//!
//! * a send that died before writing its manifest, whose snapshot was later
//!   destroyed locally — nothing will ever resume it;
//! * chunks left beside a manifest that does not reference them.
//!
//! A snapshot whose resume marker still holds a live lease is left alone:
//! that is a send in progress, not garbage.

use std::collections::{BTreeMap, BTreeSet};

use crate::manifest::{Manifest, Pending, keys};

use super::{Conn, target};

pub async fn run(uri: &str, dry_run: bool, conn: &Conn) -> anyhow::Result<()> {
    let t = target(uri, conn)?;
    let root = keys::all_manifests_prefix(&t.prefix);

    // Group every object by the snapshot directory it belongs to.
    let mut groups: BTreeMap<(String, String), Vec<(String, u64)>> = BTreeMap::new();
    for (key, size) in t.store.list(&root).await? {
        let rel = key.strip_prefix(&root).unwrap_or(key.as_str());
        let parts: Vec<&str> = rel.split('/').collect();
        // <dataset-guid>/<snapshot-guid>/<file>; pins live at pins/<guid>.
        if parts.len() != 3 || parts[0] == "pins" {
            continue;
        }
        groups
            .entry((parts[0].to_string(), parts[1].to_string()))
            .or_default()
            .push((key, size));
    }

    let mut doomed: Vec<(String, u64)> = Vec::new();
    let mut in_progress = 0usize;

    for ((ds, snap), files) in &groups {
        let name = |f: &str| format!("{root}{ds}/{snap}/{f}");
        let manifest_key = name("manifest.json");
        let pending_key = name("pending.json");
        let has_manifest = files.iter().any(|(k, _)| *k == manifest_key);

        if !has_manifest {
            // An interrupted send: garbage only once its lease has lapsed.
            if files.iter().any(|(k, _)| *k == pending_key) {
                match t.store.get(&pending_key).await {
                    Ok(b) => match Pending::decode(&b) {
                        Ok(p) if p.lease_live(time::OffsetDateTime::now_utc()) => {
                            println!(
                                "skipping {ds}/{snap}: a send is in progress (run {})",
                                p.run_id
                            );
                            in_progress += 1;
                            continue;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, "unreadable resume marker; treating as abandoned")
                        }
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, "could not read resume marker; treating as abandoned")
                    }
                }
            }
            doomed.extend(files.iter().cloned());
            continue;
        }

        // A manifest is present: anything it does not name is garbage.
        let bytes = t.store.get(&manifest_key).await?;
        let m = Manifest::decode(&bytes)
            .map_err(|e| anyhow::anyhow!("{manifest_key}: manifest is unreadable: {e}"))?;
        let mut expected: BTreeSet<String> = m
            .chunks
            .iter()
            .map(|c| keys::chunk(&t.prefix, m.dataset_guid, m.snapshot_guid, c.seq))
            .collect();
        expected.insert(manifest_key.clone());
        // A stale marker beside a committed manifest is also removable.
        for (k, size) in files {
            if !expected.contains(k) {
                doomed.push((k.clone(), *size));
            }
        }
    }

    if doomed.is_empty() {
        println!(
            "nothing to clean ({} snapshot dir(s) checked{})",
            groups.len(),
            if in_progress > 0 {
                format!(", {in_progress} in progress")
            } else {
                String::new()
            }
        );
        return Ok(());
    }

    let total: u64 = doomed.iter().map(|(_, s)| s).sum();
    for (key, size) in &doomed {
        println!(
            "{} {key} ({size} bytes)",
            if dry_run { "would remove" } else { "removing" }
        );
        if !dry_run {
            t.store.delete(key).await?;
        }
    }
    println!(
        "{}: {} object(s), {total} bytes{}",
        if dry_run { "dry run" } else { "cleaned" },
        doomed.len(),
        if dry_run { " (nothing removed)" } else { "" }
    );
    Ok(())
}

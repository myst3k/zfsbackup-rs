//! `list`: what the bucket holds, from manifests alone.

use super::{Conn, target};

pub async fn run(uri: &str, dataset: Option<&str>, conn: &Conn) -> anyhow::Result<()> {
    let t = target(uri, conn)?;
    let manifests = t.manifests().await?;
    let pinned = t.pinned().await?;
    let matches = |name: &str| match dataset {
        None => true,
        Some(d) => match d.strip_suffix('*') {
            Some(p) => name.starts_with(p),
            None => name == d,
        },
    };
    let mut shown = 0usize;
    println!(
        "{:<40} {:>16} {:<12} {:>14} {:>7}  {}",
        "SNAPSHOT", "GUID", "KIND", "BYTES", "CHUNKS", "CREATED"
    );
    for m in &manifests {
        if !matches(&m.dataset) {
            continue;
        }
        shown += 1;
        let kind = match m.from_guid {
            None => "full".to_string(),
            Some(base) => format!("incr←{base}"),
        };
        let pin = if pinned.contains(&m.snapshot_guid) {
            " [pinned]"
        } else {
            ""
        };
        println!(
            "{:<40} {:>16} {:<12} {:>14} {:>7}  {}{}",
            m.snapshot,
            m.snapshot_guid.to_string(),
            kind,
            m.bytes,
            m.chunks.len(),
            m.created_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| m.created_at.to_string()),
            pin
        );
    }
    if shown == 0 {
        println!(
            "(nothing archived{})",
            dataset.map(|d| format!(" for {d}")).unwrap_or_default()
        );
    }
    Ok(())
}

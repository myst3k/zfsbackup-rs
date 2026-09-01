//! `check`: does this endpoint and bucket actually behave the way backups
//! need it to?
//!
//! Everything here is measured against the live endpoint rather than assumed:
//! the CRC32C probe uploads a part with a correct checksum and then a wrong
//! one, and only reports verification when the wrong one is refused.

use crate::manifest::keys;

use super::{Conn, target};

pub async fn run(uri: &str, conn: &Conn) -> anyhow::Result<()> {
    let t = target(uri, conn)?;
    println!("checking {} …", t.store.label());
    let r = t
        .store
        .validate(&keys::all_manifests_prefix(&t.prefix))
        .await?;

    let yn = |b: bool| if b { "yes" } else { "no" };
    let tri = |b: Option<bool>| match b {
        Some(true) => "yes".to_string(),
        Some(false) => "NO".to_string(),
        None => "unknown".to_string(),
    };

    println!(
        "  reachable            {} ({})",
        yn(r.reachable),
        r.latency_ms
            .map(|m| format!("{m} ms"))
            .unwrap_or_else(|| "—".into())
    );
    println!("  credentials          {}", yn(r.credentials_ok));
    println!("  bucket               {}", yn(r.bucket_exists));
    println!(
        "  versioning           {}",
        r.versioning.as_deref().unwrap_or("unknown")
    );
    println!(
        "  object lock          {}{}",
        tri(r.object_lock),
        r.object_lock_default_retention
            .as_deref()
            .map(|d| format!(" ({d})"))
            .unwrap_or_default()
    );
    println!(
        "  lifecycle abort MPU  {}",
        r.lifecycle_abort_mpu_days
            .map(|d| format!("{d} days"))
            .unwrap_or_else(|| "not configured".into())
    );
    println!(
        "  write / read / delete  {} / {} / {}",
        yn(r.can_write),
        yn(r.can_read),
        yn(r.can_delete)
    );
    println!("  multipart            {}", yn(r.can_multipart));
    println!(
        "  CRC32C verified      {}   (chunk uploads are checked by the store)",
        tri(r.crc32c_verified)
    );
    println!("  CRC64NVME verified   {}", tri(r.crc64nvme_verified));

    if r.crc32c_verified != Some(true) {
        println!(
            "\nThis endpoint does not verify CRC32C on upload. Backups still carry\n\
             BLAKE3 in their manifests, so `verify` and `receive` detect corruption —\n\
             but it is caught on read rather than refused on write."
        );
    }
    for w in &r.warnings {
        println!("  warning: {w}");
    }
    for e in &r.errors {
        println!("  error:   {e}");
    }
    if r.ok {
        println!("\nusable for backups");
        Ok(())
    } else {
        anyhow::bail!(
            "{} is not usable for backups (see errors above)",
            t.store.label()
        )
    }
}

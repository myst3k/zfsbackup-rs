//! `pin` / `unpin`: exempt a snapshot (and, through the retention rules,
//! everything it depends on) from deletion. A pin is a marker object in the
//! bucket, so it survives anything short of deleting the bucket.

use bytes::Bytes;

use crate::manifest::keys;

use super::{Conn, target};

pub async fn run(snapshot: &str, uri: &str, pin: bool, conn: &Conn) -> anyhow::Result<()> {
    let t = target(uri, conn)?;
    let m = t.manifest_for(snapshot).await?;
    let key = keys::pin(&t.prefix, m.snapshot_guid);
    if pin {
        t.store
            .put(&key, Bytes::from(m.snapshot.clone().into_bytes()))
            .await?;
        println!("pinned {snapshot}");
    } else {
        t.store.delete(&key).await?;
        println!("unpinned {snapshot}");
    }
    Ok(())
}

# zfsbackup-rs

Back up `zfs send` streams to any S3-compatible store. Single static binary,
no server, no agent daemon, no database — the bucket is the source of truth.

**Status: early development. Not yet ready for production data.**

## Why another one

- **Checksummed end to end.** The ZFS stream's own fletcher4 record checksums
  are verified as the stream is read; every chunk carries a BLAKE3 hash in the
  manifest; uploads use `x-amz-checksum-crc32c` so the store verifies each
  part server-side and refuses corruption at write time.
- **Resumable.** An interrupted backup continues where it stopped instead of
  starting over.
- **Dependency-aware retention.** `retention` expires snapshots by age and
  count but never deletes a full whose incrementals still exist, and never
  breaks a restore chain. Pins exempt a snapshot entirely.
- **Verifiable without ZFS.** `verify` re-downloads and re-hashes a backup
  without touching a pool.
- Streams are chunked, not staged: no local temp copies of your data.
- Written in Rust; CPU cost per byte is low enough to not be the bottleneck.

## Quick start

```sh
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…
export ZB_ENDPOINT=https://s3.us-east-2.wasabisys.com ZB_REGION=us-east-2

zfsbackup-rs send   tank/data@monday  s3://my-backups     # full
zfsbackup-rs send   tank/data@tuesday s3://my-backups     # incremental, base picked automatically
zfsbackup-rs list   s3://my-backups
zfsbackup-rs verify tank/data@tuesday s3://my-backups
zfsbackup-rs receive tank/data@tuesday s3://my-backups pool2/restored
zfsbackup-rs retention s3://my-backups --keep-last 30 --older-than 90d
```

## License

MIT or Apache-2.0, at your option.

## Roadmap

- Cheap remote audit: check stored CRC32C via `GetObjectAttributes` without
  downloading (today `verify` re-downloads everything, which is the strongest
  check but costs egress).
- Prune ported-but-unused engine code; CI; static release binaries.

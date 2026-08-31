# zfsbackup-rs

Back up `zfs send` streams to any S3-compatible object store. A single
static binary that treats the bucket as the entire database — every command
works from object storage alone.

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

> **Status: early development.** The core workflow (send, receive, list,
> verify, retention, pins) is implemented and exercised against real ZFS
> pools and a real S3 provider (Wasabi), but the tool is young. Do not make
> it the only copy of data you care about yet.

## Why

Integrity comes first: every byte is verified on its way in, provable at
rest, and verified again on its way out.

- **Verified at every hop.**
  1. While reading: every record's fletcher4 checksum in the `zfs send`
     stream is verified as it flows.
  2. In transit: every chunk is uploaded with `x-amz-checksum-crc32c`; the
     store verifies the body server-side and refuses a mismatch, so a
     corrupt upload cannot land.
  3. At rest: the manifest records size and BLAKE3 for every chunk and for
     the whole stream.
  4. On restore: every chunk is BLAKE3-checked before a byte reaches
     `zfs receive`, and ZFS then re-verifies its own stream checksums.
- **`verify` without ZFS.** Re-download and re-hash any backup from any
  machine with the credentials. Put it in cron and find bit rot
  early, while a restore is still a drill.
- **Chain-safe retention.** Expire by age and count; a full stays until
  every incremental that depends on it, directly or transitively, is gone.
  Pins exempt a snapshot and, through the rules, its whole ancestry.
- **Crash-safe by construction.** The manifest is written last, so an
  interrupted backup stays invisible; retention deletes the manifest first,
  so every listed backup has all of its chunks. An interrupted send resumes
  past chunks already uploaded.
- **Zero staging.** Chunks stream straight from the `zfs send` pipe to the
  store, keeping local disk out of the data path.
- **Incrementals that survive snapshot rotation.** After each send the tool
  leaves a bookmark; the base snapshot can be destroyed and future
  incrementals still work.

## Install

Prebuilt static binaries are planned. Until then:

```sh
cargo install --git https://github.com/myst3k/zfsbackup-rs
```

Requires a `zfs` binary on the host for `send`/`receive`; `list`, `verify`,
`retention` and `pin` run anywhere.

## Quick start

```sh
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…
export ZB_ENDPOINT=https://s3.us-east-2.wasabisys.com
export ZB_REGION=us-east-2

zfs snapshot tank/data@$(date -u +%Y%m%dT%H%M%SZ)

zfsbackup-rs send tank/data@20260831T120000Z s3://my-backups
# first run: full. later runs: incremental from the newest archived base,
# found automatically via snapshots or the bookmarks the tool maintains.

zfsbackup-rs list    s3://my-backups
zfsbackup-rs verify  tank/data@20260831T120000Z s3://my-backups
zfsbackup-rs receive tank/data@20260831T120000Z s3://my-backups pool2/restored
# receives the full chain (full + intermediate incrementals), oldest first.

zfsbackup-rs retention s3://my-backups --keep-last 30 --older-than 90d --dry-run
zfsbackup-rs pin tank/data@20260831T120000Z s3://my-backups
```

A path after the bucket scopes everything to a prefix:
`s3://my-backups/hosts/alpha`.

## Commands

| command | what it does |
|---|---|
| `send <snap> <uri>` | Archive one snapshot. `--from` picks an explicit base, `--full` forces a full, `--chunk-size`/`--parallel` tune the upload. Resumes an interrupted run. |
| `receive <snap> <uri> <dataset>` | Restore the snapshot and everything it depends on into a dataset. `--force` passes `-F`, `--window` sets prefetch depth. |
| `list <uri>` | Every archived snapshot, its kind (full / incremental + base), size, chunk count, pins. `--dataset` filters (trailing `*` for a prefix). |
| `verify <snap> <uri>` | Download and re-hash every chunk; compare sizes, per-chunk BLAKE3 and whole-stream BLAKE3 against the manifest. Writes nothing. |
| `retention <uri>` | Delete what `--older-than` and `--keep-last` allow — minus pins, minus anything a kept snapshot depends on. `--dry-run` prints the plan. |
| `pin` / `unpin <snap> <uri>` | Exempt a snapshot from retention. Pins are marker objects in the bucket. |

Credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`;
endpoint and region from `--endpoint`/`--region`, `ZB_ENDPOINT`/`ZB_REGION`,
or `AWS_ENDPOINT_URL`/`AWS_REGION`.

## How it is stored

Keys are relative to the bucket (or to the prefix in
`s3://bucket/prefix`): `zb/` namespaces everything the tool writes, so a
bucket can hold other data alongside, and `v1/` versions the layout.

```text
zb/v1/<dataset-guid>/<snapshot-guid>/manifest.json
zb/v1/<dataset-guid>/<snapshot-guid>/chunk-000000 … chunk-NNNNNN
zb/v1/pins/<snapshot-guid>
```

Keys are GUID-based, so renaming a dataset never orphans its history; human
names live inside the manifests. Everything the tool knows is
derivable from the bucket: copy the bucket and you have copied the whole
backup system. Encrypted datasets are sent `--raw` automatically, so only
ciphertext ever leaves the host.

## Requirements

- OpenZFS 2.x on hosts that run `send`/`receive`.
- An S3-compatible store. Endpoints that verify `x-amz-checksum-crc32c`
  (AWS S3, Wasabi, and others) give write-time verification; on endpoints
  that do not, integrity still holds via the BLAKE3 manifest checks.
- Enough snapshot/bookmark/hold delegation (`zfs allow`) or root on the host.

## Roadmap

- Cheap remote audit: check stored checksums via `GetObjectAttributes`
  without downloading (today `verify` re-downloads everything — the
  strongest check, but it costs egress).
- CI, static release binaries (x86-64 and arm64, musl), crates.io release.
- Prune remaining ported-but-unused engine code.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Contributions are welcome under
the same terms.

//! Bucket administration and validation for S3-compatible endpoints.
//!
//! `object_store` covers objects; bucket-level operations (create, versioning,
//! Object Lock, lifecycle) need raw S3 REST calls. This is a minimal SigV4
//! client over `reqwest` — no AWS SDK.

use std::time::{Duration, Instant};

use bytes::Bytes;

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::{debug, instrument};

use crate::store::S3Config;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{op}: {status} {code}: {message}")]
    S3 {
        op: &'static str,
        status: u16,
        code: String,
        message: String,
    },
    #[error("bad endpoint: {0}")]
    Endpoint(String),
}

pub type Result<T> = std::result::Result<T, AdminError>;

/// Validation report, stored on the storage target and shown in the console.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Report {
    pub ok: bool,
    pub reachable: bool,
    pub credentials_ok: bool,
    pub bucket_exists: bool,
    pub versioning: Option<String>,
    pub object_lock: Option<bool>,
    pub object_lock_default_retention: Option<String>,
    pub lifecycle_abort_mpu_days: Option<u32>,
    pub can_write: bool,
    pub can_read: bool,
    pub can_delete: bool,
    pub can_multipart: bool,
    pub latency_ms: Option<u64>,
    /// The target verifies `x-amz-checksum-crc32c` on multipart parts
    /// (probe: wrong checksum refused, echo matches, complete accepted).
    pub crc32c_verified: Option<bool>,
    pub crc64nvme_verified: Option<bool>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub checked_at: String,
}

/// Outcome of [`Admin::probe_checksums`].
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ChecksumProbe {
    pub crc32c_composite: ChecksumProbeResult,
    pub crc64nvme_full_object: ChecksumProbeResult,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ChecksumProbeResult {
    pub algorithm: String,
    pub upload_part_accepted: bool,
    /// The endpoint returned the checksum header on UploadPart and it matched.
    pub upload_part_echoes_checksum: Option<bool>,
    /// A wrong checksum was refused (true = the endpoint verifies).
    pub wrong_checksum_rejected: Option<bool>,
    pub wrong_checksum_error_code: Option<String>,
    pub complete_accepted: bool,
    pub complete_checksum: Option<String>,
    pub complete_checksum_type: Option<String>,
    pub full_object_checksum_matches: Option<bool>,
    pub error: Option<String>,
}

pub struct Admin {
    http: reqwest::Client,
    cfg: S3Config,
    scheme: String,
    host: String,
}

impl Admin {
    pub fn new(cfg: &S3Config) -> Result<Self> {
        let (scheme, host) = cfg
            .endpoint
            .split_once("://")
            .ok_or_else(|| AdminError::Endpoint(cfg.endpoint.clone()))?;
        if scheme == "http" && !cfg.allow_http {
            return Err(AdminError::Endpoint(
                "http endpoint but allow_http is false".into(),
            ));
        }
        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(600))
            .danger_accept_invalid_certs(crate::store::dev_allow_invalid_certs())
            .build()?;
        Ok(Self {
            http,
            cfg: cfg.clone(),
            scheme: scheme.to_string(),
            host: host.trim_end_matches('/').to_string(),
        })
    }

    /// (host header value, path) for a bucket-level or object request.
    fn addr(&self, key: Option<&str>) -> (String, String) {
        let key = key.map(|k| format!("/{k}")).unwrap_or_default();
        if self.cfg.path_style {
            (self.host.clone(), format!("/{}{}", self.cfg.bucket, key))
        } else {
            (
                format!("{}.{}", self.cfg.bucket, self.host),
                if key.is_empty() { "/".into() } else { key },
            )
        }
    }

    async fn call(
        &self,
        op: &'static str,
        method: reqwest::Method,
        key: Option<&str>,
        query: &str,
        body: Vec<u8>,
        extra_headers: &[(&str, String)],
    ) -> Result<(u16, String)> {
        let (status, _, text) = self
            .call_full(op, method, key, query, body, extra_headers)
            .await?;
        Ok((status, text))
    }

    /// Like `call`, also returning the response headers.
    async fn call_full(
        &self,
        op: &'static str,
        method: reqwest::Method,
        key: Option<&str>,
        query: &str,
        body: Vec<u8>,
        extra_headers: &[(&str, String)],
    ) -> Result<(u16, reqwest::header::HeaderMap, String)> {
        let payload_hash = hex::encode(Sha256::digest(&body));
        self.call_body(
            op,
            method,
            key,
            query,
            body.into(),
            payload_hash,
            extra_headers,
        )
        .await
    }

    /// Data-path call: the body is sent as-is (`Bytes`, no copy) with an
    /// unsigned payload — over TLS the SigV4 body hash adds a SHA-256 pass
    /// for nothing, which is why every S3 client uses UNSIGNED-PAYLOAD here.
    async fn call_unsigned(
        &self,
        op: &'static str,
        method: reqwest::Method,
        key: Option<&str>,
        query: &str,
        body: Bytes,
        extra_headers: &[(&str, String)],
    ) -> Result<(u16, reqwest::header::HeaderMap, String)> {
        self.call_body(
            op,
            method,
            key,
            query,
            body,
            "UNSIGNED-PAYLOAD".to_string(),
            extra_headers,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all, fields(method, path, query))]
    async fn call_body(
        &self,
        op: &'static str,
        method: reqwest::Method,
        key: Option<&str>,
        query: &str,
        body: Bytes,
        payload_hash: String,
        extra_headers: &[(&str, String)],
    ) -> Result<(u16, reqwest::header::HeaderMap, String)> {
        let (host, path) = self.addr(key);
        let now = time::OffsetDateTime::now_utc();
        let amz_date = now
            .format(&time::macros::format_description!(
                "[year][month][day]T[hour][minute][second]Z"
            ))
            .unwrap();
        let date = &amz_date[..8];

        let mut headers: Vec<(String, String)> = vec![
            ("host".into(), host.clone()),
            ("x-amz-content-sha256".into(), payload_hash.clone()),
            ("x-amz-date".into(), amz_date.clone()),
        ];
        for (k, v) in extra_headers {
            headers.push((k.to_lowercase(), v.trim().to_string()));
        }
        headers.sort();
        let signed_headers: Vec<String> = headers.iter().map(|(k, _)| k.clone()).collect();
        let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
        let canonical_query = canonical_query(query);
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            uri_encode_path(&path),
            canonical_query,
            canonical_headers,
            signed_headers.join(";"),
            payload_hash
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.cfg.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let k_date = hmac(
            format!("AWS4{}", self.cfg.secret_access_key).as_bytes(),
            date.as_bytes(),
        );
        let k_region = hmac(&k_date, self.cfg.region.as_bytes());
        let k_service = hmac(&k_region, b"s3");
        let k_signing = hmac(&k_service, b"aws4_request");
        let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={}, Signature={signature}",
            self.cfg.access_key_id,
            signed_headers.join(";")
        );

        // The wire query must be encoded the same way the canonical query
        // is (RFC 3986), or a marker containing '+' or '=' breaks the
        // signature on the server side.
        let wire_query: String = query
            .split('&')
            .filter(|kv| !kv.is_empty())
            .map(|kv| {
                let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
                format!("{}={}", uri_encode(k, true), uri_encode(v, true))
            })
            .collect::<Vec<_>>()
            .join("&");
        let url = format!(
            "{}://{host}{path}{}",
            self.scheme,
            if wire_query.is_empty() {
                String::new()
            } else {
                format!("?{wire_query}")
            }
        );
        let mut req = self
            .http
            .request(method, &url)
            .header("authorization", authorization);
        for (k, v) in &headers {
            if k != "host" {
                req = req.header(k.as_str(), v.as_str());
            }
        }
        let resp = req
            .header("content-length", body.len().to_string())
            .body(body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let resp_headers = resp.headers().clone();
        let text = match resp.text().await {
            Ok(t) => t,
            Err(e) => format!("<unreadable response body: {e}>"),
        };
        debug!(status, "s3 admin call");
        if (200..300).contains(&status) {
            Ok((status, resp_headers, text))
        } else {
            Err(AdminError::S3 {
                op,
                status,
                code: xml_tag(&text, "Code").unwrap_or_else(|| status.to_string()),
                message: xml_tag(&text, "Message").unwrap_or_default(),
            })
        }
    }

    /// UploadPart carrying `x-amz-checksum-crc32c`. The store verifies the
    /// body against it (BadDigest on mismatch) and echoes it back, which we
    /// check too. Returns the part's ETag. `part_number` is 1-based.
    pub async fn upload_part_crc32c(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
        crc: u32,
    ) -> Result<String> {
        let want = crate::store::crc32c_b64(crc);
        let (_, headers, _) = self
            .call_unsigned(
                "UploadPart",
                reqwest::Method::PUT,
                Some(key),
                &format!("partNumber={part_number}&uploadId={upload_id}"),
                data,
                &[
                    ("x-amz-sdk-checksum-algorithm", "CRC32C".into()),
                    ("x-amz-checksum-crc32c", want.clone()),
                ],
            )
            .await?;
        let echoed = headers
            .get("x-amz-checksum-crc32c")
            .and_then(|v| v.to_str().ok());
        if echoed != Some(want.as_str()) {
            return Err(AdminError::S3 {
                op: "UploadPart",
                status: 200,
                code: "ChecksumNotEchoed".into(),
                message: format!(
                    "store returned checksum {echoed:?}, sent {want}; target does not verify CRC32C"
                ),
            });
        }
        headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .ok_or_else(|| AdminError::S3 {
                op: "UploadPart",
                status: 200,
                code: "NoETag".into(),
                message: "UploadPart response had no ETag".into(),
            })
    }

    /// PUT one object carrying `x-amz-checksum-crc32c`: the store verifies
    /// the body server-side (BadDigest on mismatch) and echoes the checksum,
    /// which we check too.
    pub async fn put_object_crc32c(&self, key: &str, data: Bytes, crc: u32) -> Result<()> {
        let want = crate::store::crc32c_b64(crc);
        let (_, headers, _) = self
            .call_unsigned(
                "PutObject",
                reqwest::Method::PUT,
                Some(key),
                "",
                data,
                &[
                    ("x-amz-sdk-checksum-algorithm", "CRC32C".into()),
                    ("x-amz-checksum-crc32c", want.clone()),
                ],
            )
            .await?;
        let echoed = headers
            .get("x-amz-checksum-crc32c")
            .and_then(|v| v.to_str().ok());
        if echoed != Some(want.as_str()) {
            return Err(AdminError::S3 {
                op: "PutObject",
                status: 200,
                code: "ChecksumNotEchoed".into(),
                message: format!(
                    "store returned checksum {echoed:?}, sent {want}; target does not verify CRC32C"
                ),
            });
        }
        Ok(())
    }

    /// CompleteMultipartUpload listing each part's ETag and CRC32C.
    pub async fn complete_multipart_crc32c(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[crate::store::CompletedPart],
    ) -> Result<()> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (i, p) in parts.iter().enumerate() {
            xml.push_str(&format!(
                "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag>",
                i + 1,
                p.etag
            ));
            if let Some(c) = p.crc32c {
                xml.push_str(&format!(
                    "<ChecksumCRC32C>{}</ChecksumCRC32C>",
                    crate::store::crc32c_b64(c)
                ));
            }
            xml.push_str("</Part>");
        }
        xml.push_str("</CompleteMultipartUpload>");
        let (_, _, body) = self
            .call_full(
                "CompleteMultipartUpload",
                reqwest::Method::POST,
                Some(key),
                &format!("uploadId={upload_id}"),
                xml.into_bytes(),
                &[],
            )
            .await?;
        // S3 can answer 200 with an error document on Complete.
        if let Some(code) = xml_tag(&body, "Code")
            && xml_tag(&body, "ETag").is_none()
        {
            return Err(AdminError::S3 {
                op: "CompleteMultipartUpload",
                status: 200,
                code,
                message: xml_tag(&body, "Message").unwrap_or_default(),
            });
        }
        Ok(())
    }

    /// Does this endpoint verify and return S3 additional checksums on
    /// multipart uploads? Runs a real CreateMultipartUpload → UploadPart
    /// (correct checksum, then a wrong one that must be rejected) →
    /// CompleteMultipartUpload → delete, for CRC32C (composite) and
    /// CRC64NVME (full object).
    pub async fn probe_checksums(&self) -> Result<ChecksumProbe> {
        let crc32c_composite = self
            .probe_one(
                crc_fast::CrcAlgorithm::Crc32Iscsi,
                "CRC32C",
                "crc32c",
                false,
            )
            .await;
        let crc64nvme_full_object = self
            .probe_one(
                crc_fast::CrcAlgorithm::Crc64Nvme,
                "CRC64NVME",
                "crc64nvme",
                true,
            )
            .await;
        Ok(ChecksumProbe {
            crc32c_composite,
            crc64nvme_full_object,
        })
    }

    async fn probe_one(
        &self,
        alg: crc_fast::CrcAlgorithm,
        alg_name: &str,
        hdr: &str,
        full_object: bool,
    ) -> ChecksumProbeResult {
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::STANDARD;
        let width = if alg_name == "CRC64NVME" { 8 } else { 4 };
        let enc = |v: u64| -> String { b64.encode(&v.to_be_bytes()[8 - width..]) };
        let key = format!("v1/.probe-{}-{}", hdr, uuid::Uuid::now_v7());
        let mut r = ChecksumProbeResult {
            algorithm: alg_name.to_string(),
            ..Default::default()
        };
        // Two 5 MiB parts of deterministic pseudo-random data.
        let part = |seed: u64| -> Vec<u8> {
            let mut v = vec![0u8; 5 << 20];
            let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ seed;
            for c in v.as_chunks_mut::<8>().0 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                *c = x.to_le_bytes();
            }
            v
        };
        let p1 = part(1);
        let p2 = part(2);
        let c1 = crc_fast::checksum(alg, &p1);
        let c2 = crc_fast::checksum(alg, &p2);
        let checksum_hdr = format!("x-amz-checksum-{hdr}");

        let mut create_headers = vec![("x-amz-checksum-algorithm", alg_name.to_string())];
        if full_object {
            create_headers.push(("x-amz-checksum-type", "FULL_OBJECT".into()));
        }
        let upload_id = match self
            .call(
                "CreateMultipartUpload",
                reqwest::Method::POST,
                Some(&key),
                "uploads=",
                vec![],
                &create_headers,
            )
            .await
        {
            Ok((_, xml)) => match xml_tag(&xml, "UploadId") {
                Some(id) => id,
                None => {
                    r.error = Some("CreateMultipartUpload returned no UploadId".into());
                    return r;
                }
            },
            Err(e) => {
                r.error = Some(format!("CreateMultipartUpload: {e}"));
                return r;
            }
        };
        let abort = |id: String, key: String| async move {
            if let Err(e) = self
                .call(
                    "AbortMultipartUpload",
                    reqwest::Method::DELETE,
                    Some(&key),
                    &format!("uploadId={id}"),
                    vec![],
                    &[],
                )
                .await
            {
                tracing::warn!(error = %e, key, "checksum probe: abort failed");
            }
        };

        // Part 1 with the correct checksum.
        let q1 = format!("partNumber=1&uploadId={upload_id}");
        let up1 = self
            .call_full(
                "UploadPart",
                reqwest::Method::PUT,
                Some(&key),
                &q1,
                p1.clone(),
                &[
                    ("x-amz-sdk-checksum-algorithm", alg_name.to_string()),
                    (&checksum_hdr, enc(c1)),
                ],
            )
            .await;
        let etag1 = match up1 {
            Ok((_, headers, _)) => {
                r.upload_part_accepted = true;
                r.upload_part_echoes_checksum = headers
                    .get(checksum_hdr.as_str())
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v == enc(c1));
                headers
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
            }
            Err(e) => {
                r.error = Some(format!("UploadPart with {alg_name}: {e}"));
                abort(upload_id, key).await;
                return r;
            }
        };
        // Part 2 with a wrong checksum: a verifying endpoint must refuse it.
        let q2 = format!("partNumber=2&uploadId={upload_id}");
        match self
            .call(
                "UploadPart",
                reqwest::Method::PUT,
                Some(&key),
                &q2,
                p2.clone(),
                &[
                    ("x-amz-sdk-checksum-algorithm", alg_name.to_string()),
                    (&checksum_hdr, enc(c2 ^ 1)),
                ],
            )
            .await
        {
            Ok(_) => r.wrong_checksum_rejected = Some(false),
            Err(AdminError::S3 { code, .. }) => {
                r.wrong_checksum_rejected = Some(true);
                r.wrong_checksum_error_code = Some(code);
            }
            Err(e) => {
                r.error = Some(format!("UploadPart (wrong checksum) transport: {e}"));
                abort(upload_id, key).await;
                return r;
            }
        }
        // Part 2 again, correctly, so the upload can complete.
        let etag2 = match self
            .call_full(
                "UploadPart",
                reqwest::Method::PUT,
                Some(&key),
                &q2,
                p2.clone(),
                &[
                    ("x-amz-sdk-checksum-algorithm", alg_name.to_string()),
                    (&checksum_hdr, enc(c2)),
                ],
            )
            .await
        {
            Ok((_, headers, _)) => headers
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string()),
            Err(e) => {
                r.error = Some(format!("UploadPart 2: {e}"));
                abort(upload_id, key).await;
                return r;
            }
        };
        let (Some(etag1), Some(etag2)) = (etag1, etag2) else {
            r.error = Some("UploadPart returned no ETag".into());
            abort(upload_id, key).await;
            return r;
        };
        let tag = format!("Checksum{alg_name}");
        let xml = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag1}</ETag><{tag}>{}</{tag}></Part>\
             <Part><PartNumber>2</PartNumber><ETag>{etag2}</ETag><{tag}>{}</{tag}></Part></CompleteMultipartUpload>",
            enc(c1),
            enc(c2)
        );
        let mut complete_headers: Vec<(&str, String)> = Vec::new();
        let full = if full_object {
            let combined = crc_fast::checksum_combine(alg, c1, c2, p2.len() as u64);
            complete_headers.push(("x-amz-checksum-type", "FULL_OBJECT".into()));
            complete_headers.push((&checksum_hdr, enc(combined)));
            Some(enc(combined))
        } else {
            None
        };
        match self
            .call_full(
                "CompleteMultipartUpload",
                reqwest::Method::POST,
                Some(&key),
                &format!("uploadId={upload_id}"),
                xml.into_bytes(),
                &complete_headers,
            )
            .await
        {
            Ok((_, headers, body)) => {
                r.complete_accepted = true;
                r.complete_checksum = headers
                    .get(checksum_hdr.as_str())
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
                    .or_else(|| xml_tag(&body, &tag));
                r.complete_checksum_type = headers
                    .get("x-amz-checksum-type")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
                    .or_else(|| xml_tag(&body, "ChecksumType"));
                if let (Some(want), Some(got)) = (&full, &r.complete_checksum) {
                    r.full_object_checksum_matches = Some(want == got);
                }
                if let Err(e) = self
                    .call(
                        "DeleteObject",
                        reqwest::Method::DELETE,
                        Some(&key),
                        "",
                        vec![],
                        &[],
                    )
                    .await
                {
                    tracing::warn!(error = %e, key, "checksum probe: delete failed");
                }
            }
            Err(e) => {
                r.error = Some(format!("CompleteMultipartUpload: {e}"));
                abort(upload_id, key).await;
            }
        }
        r
    }

    /// Every object version and delete marker under `prefix`: (key, version id).
    pub async fn list_object_versions(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        let mut key_marker = String::new();
        let mut vid_marker = String::new();
        loop {
            let mut q = format!("versions=&prefix={prefix}");
            if !key_marker.is_empty() {
                q.push_str(&format!("&key-marker={key_marker}"));
                if !vid_marker.is_empty() {
                    q.push_str(&format!("&version-id-marker={vid_marker}"));
                }
            }
            let (_, xml) = self
                .call(
                    "ListObjectVersions",
                    reqwest::Method::GET,
                    None,
                    &q,
                    vec![],
                    &[],
                )
                .await?;
            for tag in ["<Version>", "<DeleteMarker>"] {
                for block in xml.split(tag).skip(1) {
                    if let (Some(k), Some(v)) = (xml_tag(block, "Key"), xml_tag(block, "VersionId"))
                    {
                        out.push((k, v));
                    }
                }
            }
            if xml_tag(&xml, "IsTruncated").as_deref() != Some("true") {
                return Ok(out);
            }
            key_marker = xml_tag(&xml, "NextKeyMarker").unwrap_or_default();
            vid_marker = xml_tag(&xml, "NextVersionIdMarker").unwrap_or_default();
            if key_marker.is_empty() {
                return Ok(out);
            }
        }
    }

    /// Delete one version (or delete marker) of a key.
    pub async fn delete_version(&self, key: &str, version_id: &str) -> Result<()> {
        let q = if version_id.is_empty() || version_id == "null" {
            String::new()
        } else {
            format!("versionId={version_id}")
        };
        self.call(
            "DeleteObject",
            reqwest::Method::DELETE,
            Some(key),
            &q,
            vec![],
            &[],
        )
        .await?;
        Ok(())
    }

    /// Server-side copy of `key` from `src_bucket` (same endpoint and
    /// credentials) into this bucket under the same key.
    pub async fn copy_from(&self, src_bucket: &str, key: &str) -> Result<()> {
        let src = format!("/{src_bucket}/{}", uri_encode(key, false));
        self.call(
            "CopyObject",
            reqwest::Method::PUT,
            Some(key),
            "",
            vec![],
            &[("x-amz-copy-source", src)],
        )
        .await?;
        Ok(())
    }

    /// Incomplete multipart uploads under `prefix`: (key, upload id).
    pub async fn list_multipart_uploads(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        let mut key_marker = String::new();
        let mut id_marker = String::new();
        loop {
            let mut q = format!("uploads=&prefix={}", prefix);
            if !key_marker.is_empty() {
                q.push_str(&format!(
                    "&key-marker={}&upload-id-marker={}",
                    key_marker, id_marker
                ));
            }
            let (_, xml) = self
                .call(
                    "ListMultipartUploads",
                    reqwest::Method::GET,
                    None,
                    &q,
                    vec![],
                    &[],
                )
                .await?;
            for block in xml.split("<Upload>").skip(1) {
                if let (Some(k), Some(id)) = (xml_tag(block, "Key"), xml_tag(block, "UploadId")) {
                    out.push((k, id));
                }
            }
            if xml_tag(&xml, "IsTruncated").as_deref() != Some("true") {
                return Ok(out);
            }
            key_marker = xml_tag(&xml, "NextKeyMarker").unwrap_or_default();
            id_marker = xml_tag(&xml, "NextUploadIdMarker").unwrap_or_default();
            if key_marker.is_empty() && id_marker.is_empty() {
                return Ok(out);
            }
        }
    }

    pub async fn head_bucket(&self) -> Result<bool> {
        match self
            .call("HeadBucket", reqwest::Method::HEAD, None, "", vec![], &[])
            .await
        {
            Ok(_) => Ok(true),
            Err(AdminError::S3 { status: 404, .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Create the bucket; `object_lock` also enables versioning (S3 semantics).
    pub async fn create_bucket(&self, object_lock: bool) -> Result<()> {
        let body = if self.cfg.region == "us-east-1" {
            Vec::new()
        } else {
            format!(
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",
                self.cfg.region
            )
            .into_bytes()
        };
        let mut hdrs = Vec::new();
        if object_lock {
            hdrs.push(("x-amz-bucket-object-lock-enabled", "true".to_string()));
        }
        self.call("CreateBucket", reqwest::Method::PUT, None, "", body, &hdrs)
            .await?;
        Ok(())
    }

    /// "Enabled", "Suspended", or "Disabled" (never enabled).
    pub async fn versioning(&self) -> Result<String> {
        let (_, xml) = self
            .call(
                "GetBucketVersioning",
                reqwest::Method::GET,
                None,
                "versioning=",
                vec![],
                &[],
            )
            .await?;
        Ok(xml_tag(&xml, "Status").unwrap_or_else(|| "Disabled".into()))
    }

    pub async fn set_versioning(&self, enabled: bool) -> Result<()> {
        let body = format!(
            "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{}</Status></VersioningConfiguration>",
            if enabled { "Enabled" } else { "Suspended" }
        );
        self.call(
            "PutBucketVersioning",
            reqwest::Method::PUT,
            None,
            "versioning=",
            body.into_bytes(),
            &[],
        )
        .await?;
        Ok(())
    }

    /// (object lock enabled, default retention description)
    pub async fn object_lock(&self) -> Result<(bool, Option<String>)> {
        match self
            .call(
                "GetObjectLockConfiguration",
                reqwest::Method::GET,
                None,
                "object-lock=",
                vec![],
                &[],
            )
            .await
        {
            Ok((_, xml)) => {
                let enabled = xml_tag(&xml, "ObjectLockEnabled").as_deref() == Some("Enabled");
                let rule = xml_tag(&xml, "Mode").map(|m| {
                    let n = xml_tag(&xml, "Days")
                        .map(|d| format!("{d} days"))
                        .or_else(|| xml_tag(&xml, "Years").map(|y| format!("{y} years")))
                        .unwrap_or_default();
                    format!("{m} {n}")
                });
                Ok((enabled, rule))
            }
            Err(AdminError::S3 { code, .. })
                if code == "ObjectLockConfigurationNotFoundError" || code == "NotFound" =>
            {
                Ok((false, None))
            }
            Err(e) => Err(e),
        }
    }

    /// Days after which incomplete multipart uploads are aborted, if a rule exists.
    pub async fn lifecycle_abort_mpu(&self) -> Result<Option<u32>> {
        match self
            .call(
                "GetBucketLifecycleConfiguration",
                reqwest::Method::GET,
                None,
                "lifecycle=",
                vec![],
                &[],
            )
            .await
        {
            Ok((_, xml)) => Ok(xml_tag(&xml, "DaysAfterInitiation").and_then(|d| d.parse().ok())),
            Err(AdminError::S3 { code, .. }) if code == "NoSuchLifecycleConfiguration" => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Install the recommended rule: abort incomplete multipart uploads after N days.
    pub async fn set_lifecycle_abort_mpu(&self, days: u32) -> Result<()> {
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>snapshift-abort-incomplete-mpu</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>{days}</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"
        );
        let md5 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            md5::Md5::digest(body.as_bytes()),
        );
        self.call(
            "PutBucketLifecycleConfiguration",
            reqwest::Method::PUT,
            None,
            "lifecycle=",
            body.into_bytes(),
            &[("content-md5", md5)],
        )
        .await?;
        Ok(())
    }

    /// Full validation: connectivity, credentials, bucket, versioning,
    /// Object Lock, lifecycle, and a write/read/delete/multipart probe.
    pub async fn validate(&self, store: Option<&crate::store::Store>) -> Report {
        let mut r = Report {
            checked_at: time::OffsetDateTime::now_utc().to_string(),
            ..Default::default()
        };
        let t0 = Instant::now();
        match self.head_bucket().await {
            Ok(exists) => {
                r.reachable = true;
                r.credentials_ok = true;
                r.bucket_exists = exists;
                r.latency_ms = Some(t0.elapsed().as_millis() as u64);
                if !exists {
                    r.errors
                        .push(format!("bucket {} does not exist", self.cfg.bucket));
                }
            }
            Err(AdminError::S3 {
                status,
                code,
                message,
                ..
            }) => {
                r.reachable = true;
                r.latency_ms = Some(t0.elapsed().as_millis() as u64);
                if status == 403 || code.contains("Signature") || code.contains("AccessKey") {
                    r.errors
                        .push(format!("credentials rejected: {code} {message}"));
                } else {
                    r.errors
                        .push(format!("HEAD bucket failed: {status} {code} {message}"));
                }
                return r;
            }
            Err(e) => {
                r.errors.push(format!("endpoint unreachable: {e}"));
                return r;
            }
        }
        if !r.bucket_exists {
            return r;
        }
        match self.versioning().await {
            Ok(v) => {
                if v != "Enabled" {
                    r.warnings.push(
                        "versioning is not enabled: deletes are immediate and unrecoverable".into(),
                    );
                }
                r.versioning = Some(v);
            }
            Err(e) => r.warnings.push(format!("could not read versioning: {e}")),
        }
        match self.object_lock().await {
            Ok((enabled, rule)) => {
                r.object_lock = Some(enabled);
                r.object_lock_default_retention = rule;
                if !enabled {
                    r.warnings
                        .push("Object Lock is not enabled: backups are not immutable".into());
                }
            }
            Err(e) => r
                .warnings
                .push(format!("could not read Object Lock config: {e}")),
        }
        match self.lifecycle_abort_mpu().await {
            Ok(Some(d)) => r.lifecycle_abort_mpu_days = Some(d),
            Ok(None) => r.warnings.push(
                "no lifecycle rule aborts incomplete multipart uploads; stale uploads will bill until the provider's own cleanup".into(),
            ),
            Err(e) => r.warnings.push(format!("could not read lifecycle: {e}")),
        }
        if let Some(store) = store {
            let key = format!("v1/.probe/{}", uuid::Uuid::now_v7());
            let payload = bytes::Bytes::from_static(b"snapshift probe");
            match store.put(&key, payload.clone()).await {
                Ok(()) => r.can_write = true,
                Err(e) => r.errors.push(format!("probe write failed: {e}")),
            }
            if r.can_write {
                match store.get(&key).await {
                    Ok(b) if b == payload => r.can_read = true,
                    Ok(_) => r.errors.push("probe read returned different bytes".into()),
                    Err(e) => r.errors.push(format!("probe read failed: {e}")),
                }
                match store.delete(&key).await {
                    Ok(()) => r.can_delete = true,
                    Err(e) => r.errors.push(format!("probe delete failed: {e}")),
                }
                let mkey = format!("v1/.probe/{}-mpu", uuid::Uuid::now_v7());
                match store.create_multipart(&mkey).await {
                    Ok(id) => {
                        r.can_multipart = true;
                        if let Err(e) = store.abort_multipart(&mkey, &id).await {
                            r.warnings
                                .push(format!("probe multipart upload not aborted: {e}"));
                        }
                    }
                    Err(e) => r.errors.push(format!("multipart create failed: {e}")),
                }
            }
        }
        r.ok = r.errors.is_empty();
        r
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = HmacSha256::new_from_slice(key).expect("hmac key");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

fn uri_encode_path(p: &str) -> String {
    p.split('/')
        .map(|seg| uri_encode(seg, false))
        .collect::<Vec<_>>()
        .join("/")
}

fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn canonical_query(q: &str) -> String {
    if q.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = q
        .split('&')
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (uri_encode(k, true), uri_encode(v, true))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_and_encoding() {
        assert_eq!(
            xml_tag("<a><Status>Enabled</Status></a>", "Status").as_deref(),
            Some("Enabled")
        );
        assert_eq!(xml_tag("<a/>", "Status"), None);
        assert_eq!(uri_encode_path("/b/k ey/x"), "/b/k%20ey/x");
        assert_eq!(canonical_query("versioning="), "versioning=");
        assert_eq!(canonical_query("b=2&a=1"), "a=1&b=2");
    }

    /// Known-answer test from the AWS SigV4 documentation (GET object).
    #[test]
    fn sigv4_signing_key() {
        let k_date = hmac(b"AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", b"20130524");
        let k_region = hmac(&k_date, b"us-east-1");
        let k_service = hmac(&k_region, b"s3");
        let k_signing = hmac(&k_service, b"aws4_request");
        let sts = "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972";
        assert_eq!(
            hex::encode(hmac(&k_signing, sts.as_bytes())),
            "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }
}

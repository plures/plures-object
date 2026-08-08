//! HTTP handlers for the S3-compatible API.
//!
//! Route mapping:
//! - `PUT    /:bucket/*key`                       → [`put_or_upload_part`]
//! - `GET    /:bucket/*key`                       → [`get_object`]  (streaming)
//! - `DELETE /:bucket/*key`                       → [`delete_object`] or AbortMultipartUpload
//! - `HEAD   /:bucket/*key`                       → [`head_object`]
//! - `GET    /:bucket`                            → [`list_objects`]
//! - `POST   /:bucket/*key?uploads`               → [`initiate_multipart_upload`]
//! - `POST   /:bucket/*key?uploadId=…`            → [`complete_multipart_upload`]

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use plures_object_core::{CompletePart, ObjectKey};
use plures_object_store::ObjectService;
use serde::Deserialize;

use crate::{
    error::{object_error_to_api, ApiError},
    xml,
};

/// Shared application state passed to every handler.
pub type AppState = Arc<ObjectService>;

/// Query parameters for multipart upload operations.
#[derive(Debug, Default, Deserialize)]
pub struct MultipartQuery {
    /// Present (even if empty) on `POST ?uploads` to initiate.
    pub uploads: Option<String>,
    /// Upload ID for UploadPart, CompleteMultipartUpload, AbortMultipartUpload.
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    /// Part number for UploadPart.
    #[serde(rename = "partNumber")]
    pub part_number: Option<u32>,
}

/// Parse a MIME type string into an `axum` [`header::HeaderValue`], falling
/// back to `application/octet-stream` on failure.
fn content_type_value(ct: Option<&str>) -> axum::http::HeaderValue {
    ct.and_then(|s| s.parse().ok())
        .unwrap_or_else(|| "application/octet-stream".parse().unwrap())
}

// ── PUT /bucket/*key ─────────────────────────────────────────────────────────

/// Store an object, or upload a part if `?partNumber=…&uploadId=…` is present.
///
/// When multipart query parameters are present, delegates to the multipart
/// upload-part flow. Otherwise, stores a complete object.
pub async fn put_or_upload_part(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // If multipart query params are present, delegate to upload_part.
    if let (Some(upload_id), Some(part_number)) = (&params.upload_id, params.part_number) {
        let resource = format!("/{bucket}/{key}");
        match svc.upload_part(upload_id, part_number, body).await {
            Ok(part) => (
                StatusCode::OK,
                [("etag", format!("\"{}\"", part.etag))],
            )
                .into_response(),
            Err(e) => object_error_to_api(e, &resource).into_response(),
        }
    } else {
        put_object(State(svc), Path((bucket, key)), headers, body).await
    }
}

/// Store a complete object (non-multipart PUT).
async fn put_object(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let object_key = ObjectKey(format!("{bucket}/{key}"));
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let resource = format!("/{bucket}/{key}");
    match svc.put_object(object_key, body, content_type).await {
        Ok(meta) => (
            StatusCode::OK,
            [
                ("etag", format!("\"{}\"", meta.etag)),
                ("content-type", "application/xml".into()),
            ],
        )
            .into_response(),
        Err(e) => object_error_to_api(e, &resource).into_response(),
    }
}

// ── GET /bucket/*key ──────────────────────────────────────────────────────────

/// Retrieve an object, streaming chunk-by-chunk.
///
/// Streams the object body directly from chunk storage without buffering the
/// entire object in memory. Returns standard S3 headers:
/// `ETag`, `Content-Length`, `Content-Type`, `Last-Modified`.
pub async fn get_object(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let object_key = ObjectKey(format!("{bucket}/{key}"));
    let resource = format!("/{bucket}/{key}");

    match svc.stream_object(&object_key).await {
        Err(e) => object_error_to_api(e, &resource).into_response(),
        Ok((meta, chunk_stream)) => {
            // Convert the chunk stream into an axum Body.
            // Each chunk item is `Result<Bytes, ObjectError>`; map errors to IO errors.
            let body_stream = chunk_stream.map_err(|e| std::io::Error::other(e.to_string()));

            let mut response_headers = HeaderMap::new();
            response_headers.insert(
                header::ETAG,
                format!("\"{}\"", meta.etag).parse().unwrap(),
            );
            response_headers.insert(
                header::CONTENT_LENGTH,
                meta.size.to_string().parse().unwrap(),
            );
            response_headers.insert(
                header::LAST_MODIFIED,
                meta.updated_at
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string()
                    .parse()
                    .unwrap(),
            );
            response_headers.insert(
                header::CONTENT_TYPE,
                content_type_value(meta.content_type.as_deref()),
            );

            (StatusCode::OK, response_headers, Body::from_stream(body_stream)).into_response()
        }
    }
}

// ── DELETE /bucket/*key ───────────────────────────────────────────────────────

/// Delete an object. Returns `204 No Content` on success.
///
/// If `?uploadId=…` is present, aborts a multipart upload instead.
pub async fn delete_object(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
) -> Response {
    // Abort multipart upload if uploadId is present.
    if let Some(upload_id) = &params.upload_id {
        let resource = format!("/{bucket}/{key}");
        return match svc.abort_multipart_upload(upload_id).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => object_error_to_api(e, &resource).into_response(),
        };
    }

    let object_key = ObjectKey(format!("{bucket}/{key}"));
    let resource = format!("/{bucket}/{key}");

    match svc.delete_object(&object_key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => object_error_to_api(e, &resource).into_response(),
    }
}

// ── HEAD /bucket/*key ─────────────────────────────────────────────────────────

/// Return object metadata without the body.
///
/// Mirrors the headers of [`get_object`] but with no response body.
pub async fn head_object(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    let object_key = ObjectKey(format!("{bucket}/{key}"));
    let resource = format!("/{bucket}/{key}");

    match svc.head_object(&object_key).await {
        Err(e) => object_error_to_api(e, &resource).into_response(),
        Ok(meta) => {
            let mut response_headers = HeaderMap::new();
            response_headers.insert(
                header::ETAG,
                format!("\"{}\"", meta.etag).parse().unwrap(),
            );
            response_headers.insert(
                header::CONTENT_LENGTH,
                meta.size.to_string().parse().unwrap(),
            );
            response_headers.insert(
                header::LAST_MODIFIED,
                meta.updated_at
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string()
                    .parse()
                    .unwrap(),
            );
            response_headers.insert(
                header::CONTENT_TYPE,
                content_type_value(meta.content_type.as_deref()),
            );

            (StatusCode::OK, response_headers).into_response()
        }
    }
}

// ── GET /bucket?prefix=…&max-keys=… ──────────────────────────────────────────

/// Query parameters for the list endpoint.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub prefix: Option<String>,
    #[serde(rename = "max-keys")]
    pub max_keys: Option<usize>,
}

/// List objects in a bucket.
///
/// Returns an S3-compatible `ListBucketResult` XML document.
pub async fn list_objects(
    State(svc): State<AppState>,
    Path(bucket): Path<String>,
    Query(params): Query<ListQuery>,
) -> Response {
    let prefix = params.prefix.as_deref().unwrap_or("");
    let max_keys = params.max_keys.unwrap_or(1000).min(1000);

    // Build the prefix filter: "bucket/" + user prefix so list is scoped to bucket.
    let full_prefix = if prefix.is_empty() {
        format!("{bucket}/")
    } else {
        format!("{bucket}/{prefix}")
    };

    match svc.list_objects_with_meta(Some(&full_prefix), Some(max_keys)).await {
        Err(e) => ApiError::internal(e.to_string()).into_response(),
        Ok((mut objects, is_truncated)) => {
            // Strip the "bucket/" prefix from keys so they are relative to the bucket,
            // matching S3 semantics.
            let strip = format!("{bucket}/");
            for obj in objects.iter_mut() {
                if let Some(stripped) = obj.key.0.strip_prefix(&strip) {
                    obj.key = plures_object_core::ObjectKey(stripped.to_string());
                }
            }
            let xml = xml::list_bucket_result(&bucket, prefix, max_keys, is_truncated, &objects);
            (
                StatusCode::OK,
                [("content-type", "application/xml")],
                xml,
            )
                .into_response()
        }
    }
}

// ── Multipart Upload Handlers ────────────────────────────────────────────────

/// POST /{bucket}/{*key}?uploads — initiate a multipart upload.
pub async fn initiate_multipart_upload(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    _headers: HeaderMap,
) -> Response {
    let object_key = ObjectKey(format!("{bucket}/{key}"));
    let resource = format!("/{bucket}/{key}");

    match svc.initiate_multipart_upload(object_key).await {
        Ok(upload_id) => {
            let body = xml::initiate_multipart_upload_result(&bucket, &key, &upload_id);
            (
                StatusCode::OK,
                [("content-type", "application/xml")],
                body,
            )
                .into_response()
        }
        Err(e) => object_error_to_api(e, &resource).into_response(),
    }
}

/// POST /{bucket}/{*key}?uploadId=… — complete a multipart upload.
pub async fn complete_multipart_upload(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
    body: Bytes,
) -> Response {
    let resource = format!("/{bucket}/{key}");
    let upload_id = match &params.upload_id {
        Some(id) => id,
        None => {
            return ApiError::bad_request("missing uploadId query parameter").into_response();
        }
    };

    // Parse the XML body for <CompleteMultipartUpload> parts.
    let parts = match parse_complete_multipart_xml(&body) {
        Ok(p) => p,
        Err(msg) => return ApiError::bad_request(msg).into_response(),
    };

    match svc.complete_multipart_upload(upload_id, parts).await {
        Ok(meta) => {
            let body = xml::complete_multipart_upload_result(&bucket, &key, &meta.etag);
            (
                StatusCode::OK,
                [("content-type", "application/xml")],
                body,
            )
                .into_response()
        }
        Err(e) => object_error_to_api(e, &resource).into_response(),
    }
}

/// DELETE /{bucket}/{*key}?uploadId=… — abort a multipart upload.
///
/// This is handled by [`delete_object`] when `?uploadId` is present.
pub async fn abort_multipart_upload(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(params): Query<MultipartQuery>,
) -> Response {
    let resource = format!("/{bucket}/{key}");
    let upload_id = match &params.upload_id {
        Some(id) => id,
        None => {
            return ApiError::bad_request("missing uploadId query parameter").into_response();
        }
    };

    match svc.abort_multipart_upload(upload_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => object_error_to_api(e, &resource).into_response(),
    }
}

/// Parse the S3 `CompleteMultipartUpload` XML body into a list of [`CompletePart`].
///
/// Accepts a simple XML format:
/// ```xml
/// <CompleteMultipartUpload>
///   <Part><PartNumber>1</PartNumber><ETag>"…"</ETag></Part>
///   <Part><PartNumber>2</PartNumber><ETag>"…"</ETag></Part>
/// </CompleteMultipartUpload>
/// ```
fn parse_complete_multipart_xml(body: &[u8]) -> Result<Vec<CompletePart>, String> {
    let text = std::str::from_utf8(body).map_err(|_| "invalid UTF-8 in request body")?;

    let mut parts = Vec::new();
    let mut remaining = text;

    while let Some(part_start) = remaining.find("<Part>") {
        let after_part = &remaining[part_start + 6..];
        let part_end = after_part
            .find("</Part>")
            .ok_or("malformed XML: missing </Part>")?;
        let part_content = &after_part[..part_end];

        let part_number = extract_xml_tag(part_content, "PartNumber")
            .ok_or("missing <PartNumber>")?
            .parse::<u32>()
            .map_err(|_| "invalid PartNumber")?;

        let etag = extract_xml_tag(part_content, "ETag")
            .ok_or("missing <ETag>")?
            .trim_matches('"')
            .to_string();

        parts.push(CompletePart { part_number, etag });
        remaining = &after_part[part_end + 7..];
    }

    if parts.is_empty() {
        return Err("no <Part> elements found".into());
    }

    Ok(parts)
}

/// Extract text content from a simple XML tag (no attributes, no nesting).
fn extract_xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

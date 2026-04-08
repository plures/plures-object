//! HTTP handlers for the S3-compatible API.
//!
//! Route mapping:
//! - `PUT    /:bucket/*key`              → [`put_object`]
//! - `GET    /:bucket/*key`              → [`get_object`]  (streaming)
//! - `DELETE /:bucket/*key`              → [`delete_object`]
//! - `HEAD   /:bucket/*key`              → [`head_object`]
//! - `GET    /:bucket`                   → [`list_objects`]

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use plures_object_core::ObjectKey;
use plures_object_store::ObjectService;
use serde::Deserialize;

use crate::{
    error::{object_error_to_api, ApiError},
    xml,
};

/// Shared application state passed to every handler.
pub type AppState = Arc<ObjectService>;

/// Parse a MIME type string into an `axum` [`header::HeaderValue`], falling
/// back to `application/octet-stream` on failure.
fn content_type_value(ct: Option<&str>) -> axum::http::HeaderValue {
    ct.and_then(|s| s.parse().ok())
        .unwrap_or_else(|| "application/octet-stream".parse().unwrap())
}

// ── PUT /bucket/*key ─────────────────────────────────────────────────────────

/// Store an object.
///
/// The request body is the raw object data. `Content-Type` is forwarded as the
/// object's MIME type. Returns `200 OK` with an `ETag` header.
pub async fn put_object(
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
pub async fn delete_object(
    State(svc): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
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

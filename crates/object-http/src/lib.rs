//! S3-compatible HTTP API for the Plures Object platform.
//!
//! Exposes [`ObjectService`] via an axum router with the following endpoints:
//!
//! | Method   | Path                             | Operation                 |
//! |----------|----------------------------------|---------------------------|
//! | `PUT`    | `/:bucket/*key`                  | PutObject                 |
//! | `GET`    | `/:bucket/*key`                  | GetObject                 |
//! | `DELETE` | `/:bucket/*key`                  | DeleteObject              |
//! | `HEAD`   | `/:bucket/*key`                  | HeadObject                |
//! | `GET`    | `/:bucket`                       | ListObjects               |
//! | `POST`   | `/:bucket/*key?uploads`          | InitiateMultipartUpload   |
//! | `PUT`    | `/:bucket/*key?partNumber&uploadId` | UploadPart             |
//! | `POST`   | `/:bucket/*key?uploadId`         | CompleteMultipartUpload   |
//! | `DELETE` | `/:bucket/*key?uploadId`         | AbortMultipartUpload      |
//!
//! Response formats match the S3 XML schema so the API passes standard S3 SDK
//! client validation.
//!
//! # Authentication
//!
//! The gateway supports pluggable authentication via the [`auth::AuthProvider`]
//! trait. Use [`make_router_with_auth`] to enable auth, or [`make_router`] for
//! an unauthenticated gateway (development/testing).
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use plures_object_http::make_router;
//! use plures_object_store::ObjectService;
//! use plures_chunkstore::MemChunkStore;
//! use plures_manifest::MemManifestStore;
//!
//! #[tokio::main]
//! async fn main() {
//!     let service = Arc::new(ObjectService::new(
//!         Arc::new(MemChunkStore::new()),
//!         Arc::new(MemManifestStore::new()),
//!     ));
//!
//!     let app = make_router(service);
//!     let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
//!     axum::serve(listener, app).await.unwrap();
//! }
//! ```

pub mod auth;
pub mod error;
pub mod handlers;
pub mod xml;

use std::sync::Arc;

use axum::{
    middleware,
    response::IntoResponse,
    routing::{delete, get, head, put},
    Router,
};
use plures_object_store::ObjectService;

use auth::{auth_middleware, AuthProvider, NoAuth};
use handlers::{
    complete_multipart_upload, delete_object, get_object, head_object,
    initiate_multipart_upload, list_objects, put_or_upload_part, AppState,
};

/// Build the S3-compatible axum [`Router`] backed by the given [`ObjectService`].
///
/// This variant uses [`NoAuth`] — all requests are allowed. For production use,
/// prefer [`make_router_with_auth`].
pub fn make_router(service: Arc<ObjectService>) -> Router {
    make_router_with_auth(service, Arc::new(NoAuth))
}

/// Build the S3-compatible axum [`Router`] with a custom [`AuthProvider`].
///
/// The auth provider is called for every incoming request. On success the
/// resulting [`auth::Principal`] is injected into request extensions.
pub fn make_router_with_auth(
    service: Arc<ObjectService>,
    auth: Arc<dyn AuthProvider>,
) -> Router {
    let state: AppState = service;

    Router::new()
        // Object-level routes — wildcard key captures everything after bucket/
        .route("/{bucket}/{*key}", put(put_or_upload_part))
        .route("/{bucket}/{*key}", get(get_object))
        .route("/{bucket}/{*key}", delete(delete_object).post(object_post))
        .route("/{bucket}/{*key}", head(head_object))
        // Bucket-level list route
        .route("/{bucket}", get(list_objects))
        .layer(middleware::from_fn_with_state(auth.clone(), auth_middleware))
        .with_state(state)
}

/// POST /{bucket}/{*key} dispatcher — routes to either InitiateMultipartUpload
/// or CompleteMultipartUpload depending on query parameters.
async fn object_post(
    state: axum::extract::State<AppState>,
    path: axum::extract::Path<(String, String)>,
    query: axum::extract::Query<handlers::MultipartQuery>,
    headers: axum::http::HeaderMap,
    body: bytes::Bytes,
) -> axum::response::Response {
    if query.uploads.is_some() {
        initiate_multipart_upload(state, path, headers).await
    } else if query.upload_id.is_some() {
        complete_multipart_upload(state, path, query, body).await
    } else {
        error::ApiError::bad_request("missing required query parameter: uploads or uploadId")
            .into_response()
    }
}

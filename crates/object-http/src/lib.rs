//! S3-compatible HTTP API for the Plures Object platform.
//!
//! Exposes [`ObjectService`] via an axum router with the following endpoints:
//!
//! | Method   | Path                             | Operation        |
//! |----------|----------------------------------|------------------|
//! | `PUT`    | `/:bucket/*key`                  | PutObject        |
//! | `GET`    | `/:bucket/*key`                  | GetObject        |
//! | `DELETE` | `/:bucket/*key`                  | DeleteObject     |
//! | `HEAD`   | `/:bucket/*key`                  | HeadObject       |
//! | `GET`    | `/:bucket`                       | ListObjects      |
//!
//! Response formats match the S3 XML schema so the API passes standard S3 SDK
//! client validation.
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

pub mod error;
pub mod handlers;
pub mod xml;

use std::sync::Arc;

use axum::{
    routing::{delete, get, head, put},
    Router,
};
use plures_object_store::ObjectService;

use handlers::{delete_object, get_object, head_object, list_objects, put_object, AppState};

/// Build the S3-compatible axum [`Router`] backed by the given [`ObjectService`].
///
/// Mount this router directly or nest it under a prefix:
///
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use plures_object_http::make_router;
/// # use plures_object_store::ObjectService;
/// # use plures_chunkstore::MemChunkStore;
/// # use plures_manifest::MemManifestStore;
/// # let svc = Arc::new(ObjectService::new(Arc::new(MemChunkStore::new()), Arc::new(MemManifestStore::new())));
/// let app = axum::Router::new().nest("/s3", make_router(svc));
/// ```
pub fn make_router(service: Arc<ObjectService>) -> Router {
    let state: AppState = service;

    Router::new()
        // Object-level routes — wildcard key captures everything after bucket/
        .route("/{bucket}/{*key}", put(put_object))
        .route("/{bucket}/{*key}", get(get_object))
        .route("/{bucket}/{*key}", delete(delete_object))
        .route("/{bucket}/{*key}", head(head_object))
        // Bucket-level list route
        .route("/{bucket}", get(list_objects))
        .with_state(state)
}

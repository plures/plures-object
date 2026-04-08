//! Core types and traits for the Plures Object + Stream platform.
//!
//! This crate defines the shared vocabulary:
//! - Content-addressed chunk identifiers
//! - Object metadata
//! - Storage traits
//! - Error types

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ── Chunk ID ────────────────────────────────────────────────────────────────

/// Content-addressed identifier for a data chunk.
///
/// Computed as `SHA-256(data)` — two chunks with identical bytes always
/// produce the same `ChunkId`, enabling deduplication at the storage layer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId(pub String);

impl ChunkId {
    /// Compute the content-addressed ID for a byte slice.
    pub fn from_data(data: &[u8]) -> Self {
        let hash = Sha256::digest(data);
        Self(hex::encode(hash))
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ── Chunk ───────────────────────────────────────────────────────────────────

/// A raw data chunk with its content-addressed ID.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub id: ChunkId,
    pub data: bytes::Bytes,
    pub size: u64,
}

impl Chunk {
    /// Create a chunk from raw bytes, computing the content-addressed ID.
    pub fn new(data: impl Into<bytes::Bytes>) -> Self {
        let data = data.into();
        let id = ChunkId::from_data(&data);
        let size = data.len() as u64;
        Self { id, data, size }
    }
}

// ── Object Key ──────────────────────────────────────────────────────────────

/// An object key — the S3-style path identifying an object in a bucket.
///
/// Examples: `photos/2026/march/sunset.jpg`, `backups/db.tar.gz`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectKey(pub String);

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<S: Into<String>> From<S> for ObjectKey {
    fn from(s: S) -> Self {
        Self(s.into())
    }
}

// ── Object Metadata ─────────────────────────────────────────────────────────

/// Metadata for a stored object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// The object key (path).
    pub key: ObjectKey,
    /// Total size in bytes.
    pub size: u64,
    /// Content type (MIME).
    pub content_type: Option<String>,
    /// Ordered list of chunk IDs that compose this object.
    pub chunks: Vec<ChunkId>,
    /// User-defined metadata (x-amz-meta-*).
    pub user_meta: std::collections::HashMap<String, String>,
    /// ETag — for single-part uploads this is the hex SHA-256;
    /// for multipart it follows the S3 convention (`md5-partcount`).
    pub etag: String,
    /// When the object was created/last replaced.
    pub created_at: DateTime<Utc>,
    /// When the object was last modified (metadata update).
    pub updated_at: DateTime<Utc>,
}

// ── Manifest ────────────────────────────────────────────────────────────────

/// A manifest maps an object to its constituent chunks.
///
/// For small objects this is a single chunk. For multipart uploads each part
/// becomes one or more chunks, and the manifest records them in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Object this manifest describes.
    pub key: ObjectKey,
    /// Parts, each containing one or more chunks.
    pub parts: Vec<ManifestPart>,
    /// Total object size (sum of all part sizes).
    pub total_size: u64,
    /// ETag of the object (SHA-256 hex for single-part; `{sha256}-{n}` for multipart).
    #[serde(default)]
    pub etag: String,
    /// Content-Type (MIME) provided at upload time.
    #[serde(default)]
    pub content_type: Option<String>,
    /// When the object was created/last replaced.
    #[serde(default = "chrono::Utc::now")]
    pub created_at: DateTime<Utc>,
    /// When the object was last modified.
    #[serde(default = "chrono::Utc::now")]
    pub updated_at: DateTime<Utc>,
}

/// A single part in a multipart manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestPart {
    /// Part number (1-indexed, S3 convention).
    pub part_number: u32,
    /// Chunks composing this part.
    pub chunks: Vec<ChunkId>,
    /// Size of this part in bytes.
    pub size: u64,
}

impl Manifest {
    /// Create a manifest for a single-part object.
    pub fn single(
        key: ObjectKey,
        chunks: Vec<ChunkId>,
        total_size: u64,
        etag: String,
        content_type: Option<String>,
    ) -> Self {
        let now = chrono::Utc::now();
        Self {
            key,
            parts: vec![ManifestPart {
                part_number: 1,
                chunks,
                size: total_size,
            }],
            total_size,
            etag,
            content_type,
            created_at: now,
            updated_at: now,
        }
    }

    /// All chunk IDs in order across all parts.
    pub fn all_chunks(&self) -> Vec<&ChunkId> {
        self.parts.iter().flat_map(|p| p.chunks.iter()).collect()
    }
}

// ── Multipart Upload ────────────────────────────────────────────────────────

/// A part that has been successfully uploaded during a multipart upload session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadedPart {
    /// Part number (1-indexed, S3 convention).
    pub part_number: u32,
    /// ETag — SHA-256 hex of the part's raw bytes.
    pub etag: String,
    /// Size of this part in bytes.
    pub size: u64,
    /// Chunks composing this part (stored via [`ChunkStorage`]).
    pub chunk_ids: Vec<ChunkId>,
}

/// A part reference provided by the caller to `complete_multipart_upload`.
///
/// The `etag` must match the value returned by `upload_part`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletePart {
    /// Part number (1-indexed).
    pub part_number: u32,
    /// ETag returned by the corresponding `upload_part` call.
    pub etag: String,
}

/// An in-progress multipart upload session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    /// Unique upload identifier (UUID v4).
    pub upload_id: String,
    /// Object key being assembled.
    pub key: ObjectKey,
    /// When this upload was initiated.
    pub created_at: DateTime<Utc>,
    /// Parts uploaded so far, keyed by part number.
    pub parts: std::collections::HashMap<u32, UploadedPart>,
}

// ── Storage Traits ──────────────────────────────────────────────────────────

/// Errors from the object storage platform.
#[derive(Debug, thiserror::Error)]
pub enum ObjectError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("chunk not found: {0}")]
    ChunkNotFound(ChunkId),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("conflict: {0}")]
    Conflict(String),
}

/// Trait for content-addressed chunk storage.
#[async_trait::async_trait]
pub trait ChunkStorage: Send + Sync {
    /// Store a chunk. If a chunk with the same ID already exists, this is a no-op.
    async fn put(&self, chunk: Chunk) -> Result<ChunkId, ObjectError>;
    /// Retrieve a chunk by ID.
    async fn get(&self, id: &ChunkId) -> Result<Chunk, ObjectError>;
    /// Check if a chunk exists.
    async fn exists(&self, id: &ChunkId) -> Result<bool, ObjectError>;
    /// Delete a chunk (used by GC).
    async fn delete(&self, id: &ChunkId) -> Result<(), ObjectError>;
}

/// Trait for manifest storage (object → chunk mapping).
#[async_trait::async_trait]
pub trait ManifestStorage: Send + Sync {
    /// Store or replace a manifest for an object key.
    async fn put(&self, manifest: Manifest) -> Result<(), ObjectError>;
    /// Get the manifest for an object key.
    async fn get(&self, key: &ObjectKey) -> Result<Manifest, ObjectError>;
    /// Delete the manifest for an object key.
    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectError>;
    /// List all object keys (with optional prefix filter).
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<ObjectKey>, ObjectError>;
}

// ── Stream Event ────────────────────────────────────────────────────────────

/// Events emitted by the object storage system.
///
/// These feed the stream engine for replication, workflows, and notifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamEvent {
    ObjectCreated {
        key: ObjectKey,
        size: u64,
        etag: String,
        timestamp: DateTime<Utc>,
    },
    ObjectDeleted {
        key: ObjectKey,
        timestamp: DateTime<Utc>,
    },
    ChunkStored {
        id: ChunkId,
        size: u64,
        timestamp: DateTime<Utc>,
    },
    ReplicationRequested {
        key: ObjectKey,
        target_peer: String,
        timestamp: DateTime<Utc>,
    },
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_is_deterministic() {
        let a = ChunkId::from_data(b"hello world");
        let b = ChunkId::from_data(b"hello world");
        assert_eq!(a, b);
    }

    #[test]
    fn different_data_different_id() {
        let a = ChunkId::from_data(b"hello");
        let b = ChunkId::from_data(b"world");
        assert_ne!(a, b);
    }

    #[test]
    fn chunk_new_computes_id_and_size() {
        let chunk = Chunk::new(b"test data".to_vec());
        assert_eq!(chunk.size, 9);
        assert_eq!(chunk.id, ChunkId::from_data(b"test data"));
    }

    #[test]
    fn manifest_single_creates_one_part() {
        let m = Manifest::single("test/obj".into(), vec![ChunkId("abc".into())], 100, "etag".into(), None);
        assert_eq!(m.parts.len(), 1);
        assert_eq!(m.parts[0].part_number, 1);
        assert_eq!(m.total_size, 100);
    }

    #[test]
    fn manifest_all_chunks_flattens_parts() {
        let m = Manifest {
            key: "k".into(),
            parts: vec![
                ManifestPart { part_number: 1, chunks: vec![ChunkId("a".into()), ChunkId("b".into())], size: 200 },
                ManifestPart { part_number: 2, chunks: vec![ChunkId("c".into())], size: 100 },
            ],
            total_size: 300,
            etag: String::new(),
            content_type: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let all = m.all_chunks();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn object_key_from_string() {
        let key: ObjectKey = "photos/sunset.jpg".into();
        assert_eq!(key.0, "photos/sunset.jpg");
    }

    #[test]
    fn stream_event_serializes() {
        let event = StreamEvent::ObjectCreated {
            key: "test".into(),
            size: 42,
            etag: "abc".into(),
            timestamp: Utc::now(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("ObjectCreated"));
    }
}

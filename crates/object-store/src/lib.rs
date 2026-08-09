//! Object Service — orchestrates chunk storage and manifests to provide
//! the S3-style object API.
//!
//! This is the main entry point for PutObject, GetObject, DeleteObject, ListObjects,
//! and the multipart upload lifecycle (Initiate, UploadPart, Complete, Abort).

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use uuid::Uuid;

use plures_object_core::{
    Chunk, ChunkId, ChunkStorage, CompletePart, Manifest, ManifestPart, ManifestStorage,
    MultipartUpload, ObjectError, ObjectKey, ObjectMeta, StreamEvent, UploadedPart,
};
use plures_stream::StreamEngine;

/// Default chunk size: 8 MiB.
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// The object service — orchestrates PutObject, GetObject, etc.
pub struct ObjectService {
    chunks: Arc<dyn ChunkStorage>,
    manifests: Arc<dyn ManifestStorage>,
    chunk_size: usize,
    /// In-progress multipart upload sessions, keyed by upload ID.
    staging: Arc<RwLock<HashMap<String, MultipartUpload>>>,
    /// Optional stream engine for emitting events on object operations.
    stream: Option<Arc<StreamEngine>>,
}

impl ObjectService {
    /// Create a new object service with the given storage backends.
    pub fn new(chunks: Arc<dyn ChunkStorage>, manifests: Arc<dyn ManifestStorage>) -> Self {
        Self {
            chunks,
            manifests,
            chunk_size: DEFAULT_CHUNK_SIZE,
            staging: Arc::new(RwLock::new(HashMap::new())),
            stream: None,
        }
    }

    /// Override the chunk size for splitting objects.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Attach a stream engine for emitting events on object operations.
    pub fn with_stream_engine(mut self, engine: Arc<StreamEngine>) -> Self {
        self.stream = Some(engine);
        self
    }

    /// Create an ObjectService with PluresDB-backed manifest storage.
    ///
    /// Uses [`plures_manifest_db::FsManifestStore`] for graph-native manifest
    /// persistence with CRDT semantics and version history.
    pub async fn with_pluresdb(
        chunks: Arc<dyn ChunkStorage>,
        db_path: impl AsRef<std::path::Path>,
    ) -> Result<Self, ObjectError> {
        let store = plures_manifest_db::FsManifestStore::new(db_path);
        Ok(Self {
            chunks,
            manifests: Arc::new(store),
            chunk_size: DEFAULT_CHUNK_SIZE,
            staging: Arc::new(RwLock::new(HashMap::new())),
            stream: None,
        })
    }

    /// **PutObject** — store an object, splitting into content-addressed chunks.
    ///
    /// Returns the object metadata including the ETag (SHA-256 of full content).
    pub async fn put_object(
        &self,
        key: impl Into<ObjectKey>,
        data: bytes::Bytes,
        content_type: Option<String>,
    ) -> Result<ObjectMeta, ObjectError> {
        let key = key.into();
        let total_size = data.len() as u64;

        // Compute ETag (full-content hash)
        let etag = hex::encode(Sha256::digest(&data));

        // Split into chunks
        let mut chunk_ids = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + self.chunk_size).min(data.len());
            let chunk_data = data.slice(offset..end);
            let chunk = Chunk::new(chunk_data);
            let id = self.chunks.put(chunk).await?;
            chunk_ids.push(id);
            offset = end;
        }

        // Create manifest
        let manifest = Manifest::single(key.clone(), chunk_ids.clone(), total_size, etag.clone(), content_type.clone());
        self.manifests.put(manifest).await?;

        let now = Utc::now();
        let meta = ObjectMeta {
            key,
            size: total_size,
            content_type,
            chunks: chunk_ids,
            user_meta: Default::default(),
            etag,
            created_at: now,
            updated_at: now,
        };

        if let Some(engine) = self.stream.clone() {
            let event = StreamEvent::ObjectCreated {
                key: meta.key.clone(),
                size: meta.size,
                etag: meta.etag.clone(),
                timestamp: now,
            };
            tokio::spawn(async move {
                engine.publish(event).await;
            });
        }

        Ok(meta)
    }

    /// **GetObject** — retrieve an object by reassembling its chunks.
    pub async fn get_object(&self, key: &ObjectKey) -> Result<(ObjectMeta, bytes::Bytes), ObjectError> {
        let manifest = self.manifests.get(key).await?;
        let all_chunk_ids: Vec<ChunkId> = manifest.all_chunks().into_iter().cloned().collect();

        let mut assembled = Vec::with_capacity(manifest.total_size as usize);
        for chunk_id in &all_chunk_ids {
            let chunk = self.chunks.get(chunk_id).await?;
            assembled.extend_from_slice(&chunk.data);
        }

        let meta = ObjectMeta {
            key: key.clone(),
            size: manifest.total_size,
            content_type: manifest.content_type,
            chunks: all_chunk_ids,
            user_meta: Default::default(),
            etag: manifest.etag,
            created_at: manifest.created_at,
            updated_at: manifest.updated_at,
        };

        Ok((meta, bytes::Bytes::from(assembled)))
    }

    /// **DeleteObject** — remove an object's manifest (chunks may be GC'd later).
    pub async fn delete_object(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        // Get manifest to find chunks (for future GC)
        let _manifest = self.manifests.get(key).await?;
        let now = Utc::now();
        // TODO: Mark chunks for GC if no other manifest references them
        self.manifests.delete(key).await?;

        if let Some(engine) = &self.stream {
            engine
                .publish(StreamEvent::ObjectDeleted {
                    key: key.clone(),
                    timestamp: now,
                })
                .await;
        }

        Ok(())
    }

    /// **ListObjects** — list all objects with an optional prefix filter.
    pub async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<ObjectKey>, ObjectError> {
        self.manifests.list(prefix).await
    }

    /// **HeadObject** — get metadata without retrieving data.
    pub async fn head_object(&self, key: &ObjectKey) -> Result<ObjectMeta, ObjectError> {
        let manifest = self.manifests.get(key).await?;
        let all_chunk_ids: Vec<ChunkId> = manifest.all_chunks().into_iter().cloned().collect();

        Ok(ObjectMeta {
            key: key.clone(),
            size: manifest.total_size,
            content_type: manifest.content_type,
            chunks: all_chunk_ids,
            user_meta: Default::default(),
            etag: manifest.etag,
            created_at: manifest.created_at,
            updated_at: manifest.updated_at,
        })
    }

    /// **StreamObject** — retrieve an object as a chunk-by-chunk async stream.
    ///
    /// Returns the object metadata and a [`futures::Stream`] that yields each
    /// chunk's bytes sequentially without buffering the entire object in memory.
    /// Callers are responsible for collecting or forwarding the stream.
    pub async fn stream_object(
        &self,
        key: &ObjectKey,
    ) -> Result<
        (
            ObjectMeta,
            impl futures::Stream<Item = Result<bytes::Bytes, ObjectError>> + Send + 'static,
        ),
        ObjectError,
    > {
        use futures::StreamExt as _;

        let manifest = self.manifests.get(key).await?;
        let all_chunk_ids: Vec<ChunkId> = manifest.all_chunks().into_iter().cloned().collect();

        let meta = ObjectMeta {
            key: key.clone(),
            size: manifest.total_size,
            content_type: manifest.content_type,
            chunks: all_chunk_ids.clone(),
            user_meta: Default::default(),
            etag: manifest.etag,
            created_at: manifest.created_at,
            updated_at: manifest.updated_at,
        };

        let chunks = self.chunks.clone();
        let stream = futures::stream::iter(all_chunk_ids).then(move |chunk_id| {
            let chunks = chunks.clone();
            async move { chunks.get(&chunk_id).await.map(|c| c.data) }
        });

        Ok((meta, stream))
    }

    /// **ListObjectsWithMeta** — list objects with full metadata (for S3 API responses).
    ///
    /// Fetches metadata for each matching key by reading its manifest. An
    /// optional `max_keys` limit is applied after prefix filtering.
    ///
    /// Returns `(objects, is_truncated)` where `is_truncated` is `true` when
    /// more objects exist beyond `max_keys`.
    pub async fn list_objects_with_meta(
        &self,
        prefix: Option<&str>,
        max_keys: Option<usize>,
    ) -> Result<(Vec<ObjectMeta>, bool), ObjectError> {
        let keys = self.manifests.list(prefix).await?;
        let limit = max_keys.unwrap_or(1000);
        // Fetch one extra to determine whether results are truncated.
        let mut metas = Vec::new();
        let mut is_truncated = false;
        for key in keys.into_iter() {
            if metas.len() == limit {
                // We have already collected the page; the extra item proves truncation.
                is_truncated = true;
                break;
            }
            match self.head_object(&key).await {
                Ok(meta) => metas.push(meta),
                // A concurrent delete between list and head is benign — skip silently.
                Err(ObjectError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok((metas, is_truncated))
    }

    // ── Multipart Upload ─────────────────────────────────────────────────────

    /// **InitiateMultipartUpload** — begin a multipart upload session.
    ///
    /// Returns a unique upload ID that must be passed to subsequent
    /// [`upload_part`](Self::upload_part), [`complete_multipart_upload`](Self::complete_multipart_upload),
    /// and [`abort_multipart_upload`](Self::abort_multipart_upload) calls.
    pub async fn initiate_multipart_upload(
        &self,
        key: impl Into<ObjectKey>,
    ) -> Result<String, ObjectError> {
        let upload_id = Uuid::new_v4().to_string();
        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            key: key.into(),
            created_at: Utc::now(),
            parts: HashMap::new(),
        };
        self.staging.write().await.insert(upload_id.clone(), upload);
        Ok(upload_id)
    }

    /// **UploadPart** — upload one part of a multipart upload.
    ///
    /// Parts are stored as content-addressed chunks via the underlying
    /// [`ChunkStorage`]. Returns an [`UploadedPart`] whose `etag` must be
    /// supplied to [`complete_multipart_upload`](Self::complete_multipart_upload).
    ///
    /// Part numbers are 1-indexed per the S3 specification.
    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        data: bytes::Bytes,
    ) -> Result<UploadedPart, ObjectError> {
        if part_number == 0 {
            return Err(ObjectError::InvalidRequest(
                "part_number must be >= 1".into(),
            ));
        }

        // Verify the upload session exists before storing any chunks.
        {
            let guard = self.staging.read().await;
            if !guard.contains_key(upload_id) {
                return Err(ObjectError::NotFound(format!("upload {upload_id}")));
            }
        }

        let part_size = data.len() as u64;
        let etag = hex::encode(Sha256::digest(&data));

        // Split the part into chunks using the configured chunk size.
        let mut chunk_ids = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + self.chunk_size).min(data.len());
            let chunk_data = data.slice(offset..end);
            let chunk = Chunk::new(chunk_data);
            let id = self.chunks.put(chunk).await?;
            chunk_ids.push(id);
            offset = end;
        }

        let part = UploadedPart {
            part_number,
            etag,
            size: part_size,
            chunk_ids,
        };

        // Record the part in the staging session.
        self.staging
            .write()
            .await
            .get_mut(upload_id)
            .ok_or_else(|| ObjectError::NotFound(format!("upload {upload_id}")))?
            .parts
            .insert(part_number, part.clone());

        Ok(part)
    }

    /// **CompleteMultipartUpload** — assemble all uploaded parts into a final object.
    ///
    /// The caller supplies an ordered list of [`CompletePart`] values; their
    /// `etag` fields are validated against the values recorded during
    /// [`upload_part`](Self::upload_part).  The assembled object is stored as a
    /// [`Manifest`] with one [`ManifestPart`] per uploaded part.
    ///
    /// The returned [`ObjectMeta`]'s `etag` follows the S3 multipart convention:
    /// `{sha256_of_concatenated_part_etags}-{part_count}`.
    pub async fn complete_multipart_upload(
        &self,
        upload_id: &str,
        parts: Vec<CompletePart>,
    ) -> Result<ObjectMeta, ObjectError> {
        if parts.is_empty() {
            return Err(ObjectError::InvalidRequest(
                "at least one part is required".into(),
            ));
        }

        // Remove the session atomically so concurrent calls see a consistent state.
        let upload = self
            .staging
            .write()
            .await
            .remove(upload_id)
            .ok_or_else(|| ObjectError::NotFound(format!("upload {upload_id}")))?;

        // Sort by part number (S3 requires ascending order but we accept any order).
        let mut complete_parts = parts;
        complete_parts.sort_by_key(|p| p.part_number);

        for pair in complete_parts.windows(2) {
            if pair[0].part_number == pair[1].part_number {
                return Err(ObjectError::InvalidRequest(format!(
                    "duplicate part number {}",
                    pair[0].part_number
                )));
            }
        }
        let mut manifest_parts: Vec<ManifestPart> = Vec::with_capacity(complete_parts.len());
        let mut total_size = 0u64;
        let mut etag_concat = String::new();

        for cp in &complete_parts {
            let uploaded = upload.parts.get(&cp.part_number).ok_or_else(|| {
                ObjectError::InvalidRequest(format!("part {} not found", cp.part_number))
            })?;

            if uploaded.etag != cp.etag {
                return Err(ObjectError::InvalidRequest(format!(
                    "ETag mismatch for part {}: expected {}, got {}",
                    cp.part_number, uploaded.etag, cp.etag
                )));
            }

            etag_concat.push_str(&uploaded.etag);
            total_size += uploaded.size;
            manifest_parts.push(ManifestPart {
                part_number: cp.part_number,
                chunks: uploaded.chunk_ids.clone(),
                size: uploaded.size,
            });
        }

        // Multipart ETag: SHA-256(concat of part ETags) + "-" + part count.
        let etag = format!(
            "{}-{}",
            hex::encode(Sha256::digest(etag_concat.as_bytes())),
            complete_parts.len()
        );

        let all_chunk_ids: Vec<ChunkId> = manifest_parts
            .iter()
            .flat_map(|p| p.chunks.iter().cloned())
            .collect();

        let now = Utc::now();
        let manifest = Manifest {
            key: upload.key.clone(),
            parts: manifest_parts,
            total_size,
            etag: etag.clone(),
            content_type: None,
            tags: std::collections::HashMap::new(),
            created_at: now,
            updated_at: now,
        };
        self.manifests.put(manifest).await?;

        let meta = ObjectMeta {
            key: upload.key,
            size: total_size,
            content_type: None,
            chunks: all_chunk_ids,
            user_meta: Default::default(),
            etag,
            created_at: now,
            updated_at: now,
        };

        if let Some(engine) = &self.stream {
            engine
                .publish(StreamEvent::ObjectCreated {
                    key: meta.key.clone(),
                    size: meta.size,
                    etag: meta.etag.clone(),
                    timestamp: now,
                })
                .await;
        }

        Ok(meta)
    }

    /// **AbortMultipartUpload** — cancel an in-progress multipart upload and
    /// remove its staging state.
    ///
    /// Chunks that were stored during [`upload_part`](Self::upload_part) calls
    /// remain in the chunk store and will be collected by a future GC pass
    /// (they may be referenced by other objects due to content deduplication).
    pub async fn abort_multipart_upload(&self, upload_id: &str) -> Result<(), ObjectError> {
        self.staging
            .write()
            .await
            .remove(upload_id)
            .ok_or_else(|| ObjectError::NotFound(format!("upload {upload_id}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plures_chunkstore::MemChunkStore;
    use plures_manifest::MemManifestStore;
    use plures_stream::StreamEngine;

    fn test_service() -> ObjectService {
        ObjectService::new(
            Arc::new(MemChunkStore::new()),
            Arc::new(MemManifestStore::new()),
        )
        .with_chunk_size(10) // Small chunks for testing
    }

    #[tokio::test]
    async fn put_and_get_small_object() {
        let svc = test_service();
        let data = bytes::Bytes::from_static(b"hello");
        let meta = svc.put_object("test/hello.txt", data.clone(), Some("text/plain".into())).await.unwrap();

        assert_eq!(meta.key.0, "test/hello.txt");
        assert_eq!(meta.size, 5);
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));

        let (got_meta, got_data) = svc.get_object(&"test/hello.txt".into()).await.unwrap();
        assert_eq!(&*got_data, b"hello");
        assert_eq!(got_meta.size, 5);
    }

    #[tokio::test]
    async fn put_splits_into_chunks() {
        let svc = test_service(); // chunk_size = 10
        let data = bytes::Bytes::from_static(b"0123456789abcdefghij"); // 20 bytes
        let meta = svc.put_object("big", data, None).await.unwrap();

        assert_eq!(meta.chunks.len(), 2); // 10 + 10
        assert_eq!(meta.size, 20);
    }

    #[tokio::test]
    async fn get_reassembles_chunks() {
        let svc = test_service();
        let data = bytes::Bytes::from("abcdefghijklmnopqrstuvwxyz".as_bytes().to_vec());
        svc.put_object("alpha", data.clone(), None).await.unwrap();

        let (_, retrieved) = svc.get_object(&"alpha".into()).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn delete_object_removes_manifest() {
        let svc = test_service();
        svc.put_object("del/me", bytes::Bytes::from_static(b"bye"), None).await.unwrap();
        svc.delete_object(&"del/me".into()).await.unwrap();
        assert!(svc.get_object(&"del/me".into()).await.is_err());
    }

    #[tokio::test]
    async fn list_objects_with_prefix() {
        let svc = test_service();
        svc.put_object("a/1", bytes::Bytes::from_static(b"x"), None).await.unwrap();
        svc.put_object("a/2", bytes::Bytes::from_static(b"y"), None).await.unwrap();
        svc.put_object("b/3", bytes::Bytes::from_static(b"z"), None).await.unwrap();

        let all = svc.list_objects(None).await.unwrap();
        assert_eq!(all.len(), 3);

        let a_only = svc.list_objects(Some("a/")).await.unwrap();
        assert_eq!(a_only.len(), 2);
    }

    #[tokio::test]
    async fn head_object_returns_meta() {
        let svc = test_service();
        svc.put_object("head/test", bytes::Bytes::from_static(b"data"), None).await.unwrap();
        let meta = svc.head_object(&"head/test".into()).await.unwrap();
        assert_eq!(meta.size, 4);
    }

    #[tokio::test]
    async fn put_overwrites_existing() {
        let svc = test_service();
        svc.put_object("k", bytes::Bytes::from_static(b"v1"), None).await.unwrap();
        svc.put_object("k", bytes::Bytes::from_static(b"v2"), None).await.unwrap();

        let (_, data) = svc.get_object(&"k".into()).await.unwrap();
        assert_eq!(&*data, b"v2");
    }

    #[tokio::test]
    async fn etag_is_sha256() {
        let svc = test_service();
        let data = bytes::Bytes::from_static(b"etag test");
        let meta = svc.put_object("e", data.clone(), None).await.unwrap();

        let expected = hex::encode(sha2::Sha256::digest(&data));
        assert_eq!(meta.etag, expected);
    }

    #[tokio::test]
    async fn dedup_across_objects() {
        let svc = test_service();
        let data = bytes::Bytes::from_static(b"shared");
        let m1 = svc.put_object("obj1", data.clone(), None).await.unwrap();
        let m2 = svc.put_object("obj2", data.clone(), None).await.unwrap();

        // Same data → same chunk IDs
        assert_eq!(m1.chunks, m2.chunks);
    }

    // ── Multipart Upload Tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn initiate_multipart_returns_upload_id() {
        let svc = test_service();
        let id = svc.initiate_multipart_upload("multi/obj").await.unwrap();
        assert!(!id.is_empty());
    }

    #[tokio::test]
    async fn upload_part_returns_etag() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/test").await.unwrap();
        let data = bytes::Bytes::from_static(b"part one data");
        let part = svc.upload_part(&uid, 1, data.clone()).await.unwrap();

        let expected_etag = hex::encode(sha2::Sha256::digest(&data));
        assert_eq!(part.etag, expected_etag);
        assert_eq!(part.part_number, 1);
        assert_eq!(part.size, 13);
    }

    #[tokio::test]
    async fn upload_part_unknown_upload_id_errors() {
        let svc = test_service();
        let result = svc
            .upload_part("nonexistent", 1, bytes::Bytes::from_static(b"x"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn upload_part_zero_part_number_errors() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/zero").await.unwrap();
        let result = svc
            .upload_part(&uid, 0, bytes::Bytes::from_static(b"x"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn complete_multipart_upload_assembles_object() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/complete").await.unwrap();

        let p1 = svc
            .upload_part(&uid, 1, bytes::Bytes::from_static(b"hello "))
            .await
            .unwrap();
        let p2 = svc
            .upload_part(&uid, 2, bytes::Bytes::from_static(b"world"))
            .await
            .unwrap();

        let meta = svc
            .complete_multipart_upload(
                &uid,
                vec![
                    plures_object_core::CompletePart { part_number: 1, etag: p1.etag },
                    plures_object_core::CompletePart { part_number: 2, etag: p2.etag },
                ],
            )
            .await
            .unwrap();

        assert_eq!(meta.size, 11);
        assert!(meta.etag.ends_with("-2")); // multipart ETag suffix

        // Verify the assembled object can be retrieved.
        let (_, data) = svc.get_object(&"mp/complete".into()).await.unwrap();
        assert_eq!(&*data, b"hello world");
    }

    #[tokio::test]
    async fn complete_multipart_etag_mismatch_errors() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/bad-etag").await.unwrap();
        svc.upload_part(&uid, 1, bytes::Bytes::from_static(b"data"))
            .await
            .unwrap();

        let result = svc
            .complete_multipart_upload(
                &uid,
                vec![plures_object_core::CompletePart {
                    part_number: 1,
                    etag: "wrong_etag".into(),
                }],
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn complete_multipart_missing_part_errors() {
        let svc = test_service();
        let uid = svc
            .initiate_multipart_upload("mp/missing-part")
            .await
            .unwrap();
        svc.upload_part(&uid, 1, bytes::Bytes::from_static(b"data"))
            .await
            .unwrap();

        // Claim part 2 exists, but it was never uploaded.
        let result = svc
            .complete_multipart_upload(
                &uid,
                vec![plures_object_core::CompletePart {
                    part_number: 2,
                    etag: "anything".into(),
                }],
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn complete_multipart_empty_parts_errors() {
        let svc = test_service();
        let uid = svc
            .initiate_multipart_upload("mp/empty-parts")
            .await
            .unwrap();
        let result = svc.complete_multipart_upload(&uid, vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn abort_multipart_upload_cleans_up() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/abort").await.unwrap();
        svc.upload_part(&uid, 1, bytes::Bytes::from_static(b"partial"))
            .await
            .unwrap();

        svc.abort_multipart_upload(&uid).await.unwrap();

        // Session is gone — abort again should error.
        assert!(svc.abort_multipart_upload(&uid).await.is_err());
        // Object was never committed.
        assert!(svc.get_object(&"mp/abort".into()).await.is_err());
    }

    #[tokio::test]
    async fn abort_unknown_upload_id_errors() {
        let svc = test_service();
        assert!(svc.abort_multipart_upload("ghost").await.is_err());
    }

    #[tokio::test]
    async fn complete_removes_session_so_second_complete_errors() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/idempotent").await.unwrap();
        let p1 = svc
            .upload_part(&uid, 1, bytes::Bytes::from_static(b"data"))
            .await
            .unwrap();
        let parts = vec![plures_object_core::CompletePart {
            part_number: 1,
            etag: p1.etag,
        }];
        svc.complete_multipart_upload(&uid, parts.clone())
            .await
            .unwrap();
        // Second complete with same uid should fail — session already consumed.
        assert!(svc.complete_multipart_upload(&uid, parts).await.is_err());
    }

    /// Integration test: upload a 25 MiB object in 5 MiB parts.
    #[tokio::test]
    async fn integration_25mb_in_5mb_parts() {
        const MB: usize = 1024 * 1024;
        const PART_SIZE: usize = 5 * MB;
        const TOTAL_SIZE: usize = 25 * MB;

        // Use a service with 1 MiB chunk size so each 5 MiB part splits into 5 chunks.
        let svc = ObjectService::new(
            Arc::new(plures_chunkstore::MemChunkStore::new()),
            Arc::new(plures_manifest::MemManifestStore::new()),
        )
        .with_chunk_size(MB);

        // Build 25 MiB of deterministic content.
        let full_data: bytes::Bytes = (0u8..=255)
            .cycle()
            .take(TOTAL_SIZE)
            .collect::<Vec<u8>>()
            .into();

        let uid = svc.initiate_multipart_upload("large/file.bin").await.unwrap();

        let mut complete_parts = Vec::new();
        for (i, offset) in (0..TOTAL_SIZE).step_by(PART_SIZE).enumerate() {
            let end = (offset + PART_SIZE).min(TOTAL_SIZE);
            let part_data = full_data.slice(offset..end);
            let part = svc
                .upload_part(&uid, (i + 1) as u32, part_data)
                .await
                .unwrap();
            complete_parts.push(plures_object_core::CompletePart {
                part_number: part.part_number,
                etag: part.etag,
            });
        }

        assert_eq!(complete_parts.len(), 5, "should have uploaded 5 parts");

        let meta = svc
            .complete_multipart_upload(&uid, complete_parts)
            .await
            .unwrap();
        assert_eq!(meta.size, TOTAL_SIZE as u64);
        assert!(meta.etag.ends_with("-5"));

        // Verify complete object is retrievable and intact.
        let (_, retrieved) = svc.get_object(&"large/file.bin".into()).await.unwrap();
        assert_eq!(retrieved.len(), TOTAL_SIZE);
        assert_eq!(retrieved, full_data);
    }

    // ── ETag Correctness Tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn etag_stable_across_get_and_head() {
        let svc = test_service();
        let data = bytes::Bytes::from_static(b"etag stability test");
        let put_meta = svc.put_object("etag/stable", data, None).await.unwrap();

        let (get_meta, _) = svc.get_object(&"etag/stable".into()).await.unwrap();
        let head_meta = svc.head_object(&"etag/stable".into()).await.unwrap();

        assert_eq!(put_meta.etag, get_meta.etag);
        assert_eq!(put_meta.etag, head_meta.etag);
    }

    #[tokio::test]
    async fn etag_deterministic_for_same_content() {
        let svc = test_service();
        let data = bytes::Bytes::from_static(b"deterministic etag");
        let m1 = svc.put_object("det/1", data.clone(), None).await.unwrap();
        let m2 = svc.put_object("det/2", data, None).await.unwrap();
        assert_eq!(m1.etag, m2.etag, "same content must produce same ETag");
    }

    #[tokio::test]
    async fn etag_differs_for_different_content() {
        let svc = test_service();
        let m1 = svc.put_object("diff/a", bytes::Bytes::from_static(b"aaa"), None).await.unwrap();
        let m2 = svc.put_object("diff/b", bytes::Bytes::from_static(b"bbb"), None).await.unwrap();
        assert_ne!(m1.etag, m2.etag);
    }

    #[tokio::test]
    async fn singlepart_etag_has_no_dash_suffix() {
        let svc = test_service();
        let meta = svc.put_object("single/e", bytes::Bytes::from_static(b"no dash"), None).await.unwrap();
        assert!(!meta.etag.contains('-'), "single-part ETag must not contain '-'");
    }

    #[tokio::test]
    async fn multipart_etag_format_is_hash_dash_count() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/etag-fmt").await.unwrap();
        let p1 = svc.upload_part(&uid, 1, bytes::Bytes::from_static(b"aaa")).await.unwrap();
        let p2 = svc.upload_part(&uid, 2, bytes::Bytes::from_static(b"bbb")).await.unwrap();
        let p3 = svc.upload_part(&uid, 3, bytes::Bytes::from_static(b"ccc")).await.unwrap();

        let meta = svc.complete_multipart_upload(&uid, vec![
            CompletePart { part_number: 1, etag: p1.etag },
            CompletePart { part_number: 2, etag: p2.etag },
            CompletePart { part_number: 3, etag: p3.etag },
        ]).await.unwrap();

        assert!(meta.etag.ends_with("-3"), "multipart ETag must end with -<part_count>");
        let parts: Vec<&str> = meta.etag.splitn(2, '-').collect();
        assert_eq!(parts[0].len(), 64, "hash prefix must be 64 hex chars (SHA-256)");
        assert!(parts[0].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn multipart_etag_depends_on_part_content() {
        let svc = test_service();

        // Upload A with parts "aaa", "bbb"
        let uid_a = svc.initiate_multipart_upload("mp/etag-a").await.unwrap();
        let p1a = svc.upload_part(&uid_a, 1, bytes::Bytes::from_static(b"aaa")).await.unwrap();
        let p2a = svc.upload_part(&uid_a, 2, bytes::Bytes::from_static(b"bbb")).await.unwrap();
        let meta_a = svc.complete_multipart_upload(&uid_a, vec![
            CompletePart { part_number: 1, etag: p1a.etag },
            CompletePart { part_number: 2, etag: p2a.etag },
        ]).await.unwrap();

        // Upload B with parts "aaa", "ccc"
        let uid_b = svc.initiate_multipart_upload("mp/etag-b").await.unwrap();
        let p1b = svc.upload_part(&uid_b, 1, bytes::Bytes::from_static(b"aaa")).await.unwrap();
        let p2b = svc.upload_part(&uid_b, 2, bytes::Bytes::from_static(b"ccc")).await.unwrap();
        let meta_b = svc.complete_multipart_upload(&uid_b, vec![
            CompletePart { part_number: 1, etag: p1b.etag },
            CompletePart { part_number: 2, etag: p2b.etag },
        ]).await.unwrap();

        assert_ne!(meta_a.etag, meta_b.etag, "different part content must produce different multipart ETags");
    }

    #[tokio::test]
    async fn multipart_etag_deterministic_for_same_parts() {
        let svc = test_service();

        let mut etags = Vec::new();
        for key in ["mp/det1", "mp/det2"] {
            let uid = svc.initiate_multipart_upload(key).await.unwrap();
            let p1 = svc.upload_part(&uid, 1, bytes::Bytes::from_static(b"part1")).await.unwrap();
            let p2 = svc.upload_part(&uid, 2, bytes::Bytes::from_static(b"part2")).await.unwrap();
            let meta = svc.complete_multipart_upload(&uid, vec![
                CompletePart { part_number: 1, etag: p1.etag },
                CompletePart { part_number: 2, etag: p2.etag },
            ]).await.unwrap();
            etags.push(meta.etag);
        }

        assert_eq!(etags[0], etags[1], "same parts must produce same multipart ETag");
    }

    #[tokio::test]
    async fn overwrite_updates_etag() {
        let svc = test_service();
        let m1 = svc.put_object("ow/e", bytes::Bytes::from_static(b"version1"), None).await.unwrap();
        let m2 = svc.put_object("ow/e", bytes::Bytes::from_static(b"version2"), None).await.unwrap();
        assert_ne!(m1.etag, m2.etag, "overwrite with different data must change ETag");

        let (get_meta, _) = svc.get_object(&"ow/e".into()).await.unwrap();
        assert_eq!(get_meta.etag, m2.etag, "get after overwrite must return new ETag");
    }

    #[tokio::test]
    async fn complete_multipart_duplicate_part_number_errors() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/dup-pn").await.unwrap();
        let p1 = svc.upload_part(&uid, 1, bytes::Bytes::from_static(b"data")).await.unwrap();

        let result = svc.complete_multipart_upload(&uid, vec![
            CompletePart { part_number: 1, etag: p1.etag.clone() },
            CompletePart { part_number: 1, etag: p1.etag },
        ]).await;
        assert!(result.is_err(), "duplicate part numbers should error");
    }

    // ── Manifest Consistency Tests ────────────────────────────────────────────

    #[tokio::test]
    async fn manifest_chunk_ids_match_stored_chunks() {
        let chunks = Arc::new(MemChunkStore::new());
        let svc = ObjectService::new(
            chunks.clone(),
            Arc::new(MemManifestStore::new()),
        ).with_chunk_size(10);

        let data = bytes::Bytes::from_static(b"0123456789abcdef"); // 16 bytes → 2 chunks
        let meta = svc.put_object("verify/chunks", data, None).await.unwrap();

        // Every chunk ID in the metadata must exist in the chunk store.
        for chunk_id in &meta.chunks {
            assert!(chunks.exists(chunk_id).await.unwrap(), "chunk {chunk_id} must exist");
        }
    }

    #[tokio::test]
    async fn manifest_total_size_equals_sum_of_chunks() {
        let chunks = Arc::new(MemChunkStore::new());
        let svc = ObjectService::new(
            chunks.clone(),
            Arc::new(MemManifestStore::new()),
        ).with_chunk_size(10);

        let data = bytes::Bytes::from("abcdefghijklmnopqrstuvwxyz".as_bytes().to_vec()); // 26 bytes
        let meta = svc.put_object("size/check", data.clone(), None).await.unwrap();

        let mut total_chunk_bytes = 0u64;
        for chunk_id in &meta.chunks {
            let chunk = chunks.get(chunk_id).await.unwrap();
            total_chunk_bytes += chunk.size;
        }
        assert_eq!(total_chunk_bytes, meta.size);
        assert_eq!(meta.size, data.len() as u64);
    }

    #[tokio::test]
    async fn manifest_round_trip_preserves_data() {
        let svc = test_service();
        let data = bytes::Bytes::from(vec![0xAB; 35]); // 35 bytes of 0xAB
        svc.put_object("rt/data", data.clone(), Some("application/octet-stream".into())).await.unwrap();

        let (meta, retrieved) = svc.get_object(&"rt/data".into()).await.unwrap();
        assert_eq!(retrieved, data, "round-trip data must be identical");
        assert_eq!(meta.content_type.as_deref(), Some("application/octet-stream"));
    }

    #[tokio::test]
    async fn multipart_manifest_chunk_order_matches_parts() {
        let chunks = Arc::new(MemChunkStore::new());
        let manifests = Arc::new(MemManifestStore::new());
        let svc = ObjectService::new(chunks.clone(), manifests.clone()).with_chunk_size(1024);

        let uid = svc.initiate_multipart_upload("mp/order").await.unwrap();
        let part1_data = bytes::Bytes::from_static(b"FIRST");
        let part2_data = bytes::Bytes::from_static(b"SECOND");
        let p1 = svc.upload_part(&uid, 1, part1_data.clone()).await.unwrap();
        let p2 = svc.upload_part(&uid, 2, part2_data.clone()).await.unwrap();

        svc.complete_multipart_upload(&uid, vec![
            CompletePart { part_number: 1, etag: p1.etag },
            CompletePart { part_number: 2, etag: p2.etag },
        ]).await.unwrap();

        // Read manifest directly and verify chunk order yields correct reassembly.
        let manifest = manifests.get(&"mp/order".into()).await.unwrap();
        assert_eq!(manifest.parts.len(), 2);
        assert_eq!(manifest.parts[0].part_number, 1);
        assert_eq!(manifest.parts[1].part_number, 2);

        let mut assembled = Vec::new();
        for chunk_id in manifest.all_chunks() {
            let chunk = chunks.get(chunk_id).await.unwrap();
            assembled.extend_from_slice(&chunk.data);
        }
        assert_eq!(&assembled, b"FIRSTSECOND");
    }

    #[tokio::test]
    async fn overwrite_manifest_reflects_new_data() {
        let manifests = Arc::new(MemManifestStore::new());
        let svc = ObjectService::new(
            Arc::new(MemChunkStore::new()),
            manifests.clone(),
        ).with_chunk_size(10);

        svc.put_object("ow/m", bytes::Bytes::from_static(b"old"), None).await.unwrap();
        let old_manifest = manifests.get(&"ow/m".into()).await.unwrap();

        svc.put_object("ow/m", bytes::Bytes::from_static(b"new content"), None).await.unwrap();
        let new_manifest = manifests.get(&"ow/m".into()).await.unwrap();

        assert_ne!(old_manifest.etag, new_manifest.etag);
        assert_ne!(old_manifest.total_size, new_manifest.total_size);
    }

    #[tokio::test]
    async fn manifest_etag_matches_metadata_etag() {
        let manifests = Arc::new(MemManifestStore::new());
        let svc = ObjectService::new(
            Arc::new(MemChunkStore::new()),
            manifests.clone(),
        ).with_chunk_size(10);

        let put_meta = svc.put_object("etag/match", bytes::Bytes::from_static(b"check"), None).await.unwrap();
        let manifest = manifests.get(&"etag/match".into()).await.unwrap();
        assert_eq!(manifest.etag, put_meta.etag, "manifest ETag must match returned metadata ETag");
    }

    #[tokio::test]
    async fn empty_object_round_trip() {
        let svc = test_service();
        let meta = svc.put_object("empty/obj", bytes::Bytes::new(), None).await.unwrap();
        assert_eq!(meta.size, 0);
        assert!(meta.chunks.is_empty());

        let (get_meta, data) = svc.get_object(&"empty/obj".into()).await.unwrap();
        assert!(data.is_empty());
        assert_eq!(get_meta.etag, meta.etag);
    }

    #[tokio::test]
    async fn multipart_parts_submitted_out_of_order() {
        let svc = test_service();
        let uid = svc.initiate_multipart_upload("mp/ooo").await.unwrap();

        // Upload part 3 first, then 1, then 2.
        let p3 = svc.upload_part(&uid, 3, bytes::Bytes::from_static(b"CCC")).await.unwrap();
        let p1 = svc.upload_part(&uid, 1, bytes::Bytes::from_static(b"AAA")).await.unwrap();
        let p2 = svc.upload_part(&uid, 2, bytes::Bytes::from_static(b"BBB")).await.unwrap();

        // Complete with parts listed out of order — service should sort them.
        let meta = svc.complete_multipart_upload(&uid, vec![
            CompletePart { part_number: 3, etag: p3.etag },
            CompletePart { part_number: 1, etag: p1.etag },
            CompletePart { part_number: 2, etag: p2.etag },
        ]).await.unwrap();

        assert!(meta.etag.ends_with("-3"));
        let (_, data) = svc.get_object(&"mp/ooo".into()).await.unwrap();
        assert_eq!(&*data, b"AAABBBCCC", "parts must be assembled in part_number order");
    }

    // ── Stream event tests ──────────────────────────────────────────────────

    fn test_service_with_stream() -> (ObjectService, Arc<StreamEngine>) {
        let engine = Arc::new(StreamEngine::new(64));
        let svc = ObjectService::new(
            Arc::new(MemChunkStore::new()),
            Arc::new(MemManifestStore::new()),
        )
        .with_chunk_size(10)
        .with_stream_engine(engine.clone());
        (svc, engine)
    }

    #[tokio::test]
    async fn put_object_emits_created_event() {
        let (svc, engine) = test_service_with_stream();
        let mut rx = engine.subscribe();

        svc.put_object("events/hello.txt", bytes::Bytes::from("hello"), None)
            .await
            .unwrap();

        let event = rx.recv().await.unwrap();
        match event {
            StreamEvent::ObjectCreated { key, size, .. } => {
                assert_eq!(key.0, "events/hello.txt");
                assert_eq!(size, 5);
            }
            _ => panic!("expected ObjectCreated"),
        }
    }

    #[tokio::test]
    async fn delete_object_emits_deleted_event() {
        let (svc, engine) = test_service_with_stream();

        svc.put_object("events/del.txt", bytes::Bytes::from("bye"), None)
            .await
            .unwrap();

        let mut rx = engine.subscribe();
        svc.delete_object(&"events/del.txt".into()).await.unwrap();

        let event = rx.recv().await.unwrap();
        match event {
            StreamEvent::ObjectDeleted { key, .. } => {
                assert_eq!(key.0, "events/del.txt");
            }
            _ => panic!("expected ObjectDeleted"),
        }
    }

    #[tokio::test]
    async fn complete_multipart_emits_created_event() {
        let (svc, engine) = test_service_with_stream();

        let upload_id = svc
            .initiate_multipart_upload("events/multi.bin")
            .await
            .unwrap();
        let p1 = svc
            .upload_part(&upload_id, 1, bytes::Bytes::from("part1"))
            .await
            .unwrap();

        let mut rx = engine.subscribe();
        svc.complete_multipart_upload(
            &upload_id,
            vec![CompletePart {
                part_number: 1,
                etag: p1.etag,
            }],
        )
        .await
        .unwrap();

        let event = rx.recv().await.unwrap();
        match event {
            StreamEvent::ObjectCreated { key, size, .. } => {
                assert_eq!(key.0, "events/multi.bin");
                assert_eq!(size, 5);
            }
            _ => panic!("expected ObjectCreated"),
        }
    }

    #[tokio::test]
    async fn no_events_without_stream_engine() {
        let svc = test_service();
        // Should not panic even without a stream engine attached.
        svc.put_object("no-stream/test.txt", bytes::Bytes::from("ok"), None)
            .await
            .unwrap();
        svc.delete_object(&"no-stream/test.txt".into())
            .await
            .unwrap();
    }
}

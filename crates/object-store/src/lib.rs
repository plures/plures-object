//! Object Service — orchestrates chunk storage and manifests to provide
//! the S3-style object API.
//!
//! This is the main entry point for PutObject, GetObject, DeleteObject, ListObjects.

use std::sync::Arc;

use chrono::Utc;
use sha2::{Digest, Sha256};

use plures_object_core::{
    Chunk, ChunkId, ChunkStorage, Manifest, ManifestStorage, ObjectError, ObjectKey, ObjectMeta,
};

/// Default chunk size: 8 MiB.
const DEFAULT_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// The object service — orchestrates PutObject, GetObject, etc.
pub struct ObjectService {
    chunks: Arc<dyn ChunkStorage>,
    manifests: Arc<dyn ManifestStorage>,
    chunk_size: usize,
}

impl ObjectService {
    /// Create a new object service with the given storage backends.
    pub fn new(chunks: Arc<dyn ChunkStorage>, manifests: Arc<dyn ManifestStorage>) -> Self {
        Self {
            chunks,
            manifests,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Override the chunk size for splitting objects.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
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
        let manifest = Manifest::single(key.clone(), chunk_ids.clone(), total_size);
        self.manifests.put(manifest).await?;

        let now = Utc::now();
        Ok(ObjectMeta {
            key,
            size: total_size,
            content_type,
            chunks: chunk_ids,
            user_meta: Default::default(),
            etag,
            created_at: now,
            updated_at: now,
        })
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

        let etag = hex::encode(Sha256::digest(&assembled));
        let now = Utc::now();

        let meta = ObjectMeta {
            key: key.clone(),
            size: manifest.total_size,
            content_type: None,
            chunks: all_chunk_ids,
            user_meta: Default::default(),
            etag,
            created_at: now,
            updated_at: now,
        };

        Ok((meta, bytes::Bytes::from(assembled)))
    }

    /// **DeleteObject** — remove an object's manifest (chunks may be GC'd later).
    pub async fn delete_object(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        // Get manifest to find chunks (for future GC)
        let _manifest = self.manifests.get(key).await?;
        // TODO: Mark chunks for GC if no other manifest references them
        self.manifests.delete(key).await?;
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
        let now = Utc::now();

        Ok(ObjectMeta {
            key: key.clone(),
            size: manifest.total_size,
            content_type: None,
            chunks: all_chunk_ids,
            user_meta: Default::default(),
            etag: String::new(), // Would need to reassemble to compute
            created_at: now,
            updated_at: now,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plures_chunkstore::MemChunkStore;
    use plures_manifest::MemManifestStore;

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
}

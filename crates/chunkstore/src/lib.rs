//! Content-addressed chunk storage.
//!
//! Chunks are stored by their SHA-256 hash. Storing the same data twice is a
//! no-op (deduplication). Two backends:
//!
//! - [`FsChunkStore`] — on-disk, sharded by first 2 hex chars of the hash
//! - [`MemChunkStore`] — in-memory, for tests

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::RwLock;

use plures_object_core::{Chunk, ChunkId, ChunkStorage, ObjectError};

// ── Filesystem ChunkStore ───────────────────────────────────────────────────

/// On-disk content-addressed chunk store.
///
/// Layout: `{root}/{first-2-hex}/{full-hash}`
///
/// Sharding by the first two hex characters of the hash distributes files
/// across 256 directories, preventing filesystem degradation at scale.
pub struct FsChunkStore {
    root: PathBuf,
}

impl FsChunkStore {
    /// Create a new filesystem chunk store at the given root directory.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn chunk_path(&self, id: &ChunkId) -> PathBuf {
        let hash = &id.0;
        let shard = &hash[..2];
        self.root.join(shard).join(hash)
    }
}

#[async_trait::async_trait]
impl ChunkStorage for FsChunkStore {
    async fn put(&self, chunk: Chunk) -> Result<ChunkId, ObjectError> {
        let path = self.chunk_path(&chunk.id);
        if path.exists() {
            return Ok(chunk.id); // Dedup — already stored
        }
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| ObjectError::Storage(format!("mkdir failed: {e}")))?;
        }
        tokio::fs::write(&path, &chunk.data)
            .await
            .map_err(|e| ObjectError::Storage(format!("write failed: {e}")))?;
        Ok(chunk.id)
    }

    async fn get(&self, id: &ChunkId) -> Result<Chunk, ObjectError> {
        let path = self.chunk_path(id);
        let data = tokio::fs::read(&path)
            .await
            .map_err(|_| ObjectError::ChunkNotFound(id.clone()))?;
        Ok(Chunk {
            id: id.clone(),
            size: data.len() as u64,
            data: data.into(),
        })
    }

    async fn exists(&self, id: &ChunkId) -> Result<bool, ObjectError> {
        Ok(self.chunk_path(id).exists())
    }

    async fn delete(&self, id: &ChunkId) -> Result<(), ObjectError> {
        let path = self.chunk_path(id);
        if path.exists() {
            tokio::fs::remove_file(&path)
                .await
                .map_err(|e| ObjectError::Storage(format!("delete failed: {e}")))?;
        }
        Ok(())
    }
}

// ── In-Memory ChunkStore ────────────────────────────────────────────────────

/// In-memory chunk store for testing.
pub struct MemChunkStore {
    chunks: Arc<RwLock<HashMap<ChunkId, bytes::Bytes>>>,
}

impl MemChunkStore {
    pub fn new() -> Self {
        Self {
            chunks: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemChunkStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ChunkStorage for MemChunkStore {
    async fn put(&self, chunk: Chunk) -> Result<ChunkId, ObjectError> {
        self.chunks.write().await.insert(chunk.id.clone(), chunk.data);
        Ok(chunk.id)
    }

    async fn get(&self, id: &ChunkId) -> Result<Chunk, ObjectError> {
        let guard = self.chunks.read().await;
        let data = guard.get(id).ok_or_else(|| ObjectError::ChunkNotFound(id.clone()))?;
        Ok(Chunk {
            id: id.clone(),
            size: data.len() as u64,
            data: data.clone(),
        })
    }

    async fn exists(&self, id: &ChunkId) -> Result<bool, ObjectError> {
        Ok(self.chunks.read().await.contains_key(id))
    }

    async fn delete(&self, id: &ChunkId) -> Result<(), ObjectError> {
        self.chunks.write().await.remove(id);
        Ok(())
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mem_put_and_get() {
        let store = MemChunkStore::new();
        let chunk = Chunk::new(b"hello world".to_vec());
        let id = store.put(chunk).await.unwrap();
        let retrieved = store.get(&id).await.unwrap();
        assert_eq!(&*retrieved.data, b"hello world");
    }

    #[tokio::test]
    async fn mem_dedup() {
        let store = MemChunkStore::new();
        let c1 = Chunk::new(b"same data".to_vec());
        let c2 = Chunk::new(b"same data".to_vec());
        let id1 = store.put(c1).await.unwrap();
        let id2 = store.put(c2).await.unwrap();
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn mem_exists() {
        let store = MemChunkStore::new();
        let chunk = Chunk::new(b"test".to_vec());
        let id = chunk.id.clone();
        assert!(!store.exists(&id).await.unwrap());
        store.put(chunk).await.unwrap();
        assert!(store.exists(&id).await.unwrap());
    }

    #[tokio::test]
    async fn mem_delete() {
        let store = MemChunkStore::new();
        let chunk = Chunk::new(b"bye".to_vec());
        let id = store.put(chunk).await.unwrap();
        store.delete(&id).await.unwrap();
        assert!(!store.exists(&id).await.unwrap());
    }

    #[tokio::test]
    async fn mem_get_not_found() {
        let store = MemChunkStore::new();
        let result = store.get(&ChunkId("nonexistent".into())).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fs_put_get_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("chunks"));
        let chunk = Chunk::new(b"persistent data".to_vec());
        let id = store.put(chunk).await.unwrap();

        let retrieved = store.get(&id).await.unwrap();
        assert_eq!(&*retrieved.data, b"persistent data");

        assert!(store.exists(&id).await.unwrap());
        store.delete(&id).await.unwrap();
        assert!(!store.exists(&id).await.unwrap());
    }

    #[tokio::test]
    async fn fs_dedup_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsChunkStore::new(dir.path().join("chunks"));
        let c1 = Chunk::new(b"dedup test".to_vec());
        let c2 = Chunk::new(b"dedup test".to_vec());
        let id1 = store.put(c1).await.unwrap();
        let id2 = store.put(c2).await.unwrap();
        assert_eq!(id1, id2);
    }
}

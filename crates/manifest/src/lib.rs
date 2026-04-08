//! Manifest storage — maps object keys to their chunk composition.
//!
//! Provides [`MemManifestStore`] for testing and as a reference implementation.
//! The production backend will use PluresDB's graph storage to persist manifests
//! as nodes with edges to their chunk records.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use plures_object_core::{Manifest, ManifestStorage, ObjectError, ObjectKey};

/// In-memory manifest store.
pub struct MemManifestStore {
    manifests: Arc<RwLock<HashMap<String, Manifest>>>,
}

impl MemManifestStore {
    pub fn new() -> Self {
        Self {
            manifests: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for MemManifestStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ManifestStorage for MemManifestStore {
    async fn put(&self, manifest: Manifest) -> Result<(), ObjectError> {
        self.manifests
            .write()
            .await
            .insert(manifest.key.0.clone(), manifest);
        Ok(())
    }

    async fn get(&self, key: &ObjectKey) -> Result<Manifest, ObjectError> {
        self.manifests
            .read()
            .await
            .get(&key.0)
            .cloned()
            .ok_or_else(|| ObjectError::NotFound(key.0.clone()))
    }

    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        self.manifests.write().await.remove(&key.0);
        Ok(())
    }

    async fn list(&self, prefix: Option<&str>) -> Result<Vec<ObjectKey>, ObjectError> {
        let guard = self.manifests.read().await;
        let keys: Vec<ObjectKey> = guard
            .keys()
            .filter(|k| match prefix {
                Some(p) => k.starts_with(p),
                None => true,
            })
            .map(|k| ObjectKey(k.clone()))
            .collect();
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plures_object_core::{ChunkId, ManifestPart};

    fn test_manifest(key: &str) -> Manifest {
        Manifest {
            key: key.into(),
            parts: vec![ManifestPart {
                part_number: 1,
                chunks: vec![ChunkId("abc123".into())],
                size: 1024,
            }],
            total_size: 1024,
            etag: "abc123etag".into(),
            content_type: None,
            tags: std::collections::HashMap::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn put_and_get() {
        let store = MemManifestStore::new();
        store.put(test_manifest("photos/a.jpg")).await.unwrap();
        let m = store.get(&"photos/a.jpg".into()).await.unwrap();
        assert_eq!(m.total_size, 1024);
    }

    #[tokio::test]
    async fn get_not_found() {
        let store = MemManifestStore::new();
        assert!(store.get(&"nope".into()).await.is_err());
    }

    #[tokio::test]
    async fn delete_removes() {
        let store = MemManifestStore::new();
        store.put(test_manifest("x")).await.unwrap();
        store.delete(&"x".into()).await.unwrap();
        assert!(store.get(&"x".into()).await.is_err());
    }

    #[tokio::test]
    async fn list_all() {
        let store = MemManifestStore::new();
        store.put(test_manifest("a/1")).await.unwrap();
        store.put(test_manifest("b/2")).await.unwrap();
        let all = store.list(None).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn list_with_prefix() {
        let store = MemManifestStore::new();
        store.put(test_manifest("photos/a")).await.unwrap();
        store.put(test_manifest("photos/b")).await.unwrap();
        store.put(test_manifest("docs/c")).await.unwrap();
        let photos = store.list(Some("photos/")).await.unwrap();
        assert_eq!(photos.len(), 2);
    }
}

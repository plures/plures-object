//! PluresDB-backed graph-native manifest storage.
//!
//! Implements [`ManifestStorage`] using a filesystem graph store where each
//! object becomes a **graph node** with typed edges:
//!
//! ```text
//! ObjectNode ──[chunks]──► ChunkId (content-addressed)
//!     │
//!     └──[previous_version]──► ObjectNode (version history linked list)
//! ```
//!
//! ## On-disk layout
//!
//! ```text
//! {root}/
//!   objects/        ← current object nodes (one JSON file per key)
//!     {sha256(key)}.json
//!   versions/       ← archived historical nodes (keyed by node UUID)
//!     {node_id}.json
//! ```
//!
//! ## CRDT semantics
//!
//! Every node carries a **Lamport sequence number** (`seq`) that increments
//! monotonically on each write.  During replication the node with the higher
//! `seq` wins (last-write-wins).  Callers that need richer merge semantics can
//! use [`FsManifestStore::merge`] to apply an incoming node only when it has a
//! strictly higher sequence number.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use uuid::Uuid;

use plures_object_core::{Manifest, ManifestStorage, ObjectError, ObjectKey};

// ── Graph node ───────────────────────────────────────────────────────────────

/// A graph node representing a stored object in PluresDB.
///
/// Properties mirror the fields of [`Manifest`]; edges connect this node to
/// its chunks and its previous version (enabling version-history traversal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectNode {
    /// Unique identifier of this graph node (UUID v4).
    pub node_id: String,
    /// Full object manifest (properties of this node).
    pub manifest: Manifest,
    /// CRDT Lamport sequence number — monotonically increasing per store.
    pub seq: u64,
    /// Edge to the previous version of this object (version history).
    ///
    /// `None` for objects that have never been overwritten.
    pub previous_version: Option<String>,
    /// Edges to the chunk identifiers composing this object.
    ///
    /// These are content-addressed SHA-256 hex strings; each can be resolved
    /// via the chunk store to obtain raw data.
    pub chunk_edges: Vec<String>,
}

// ── FsManifestStore ──────────────────────────────────────────────────────────

/// Filesystem-backed PluresDB manifest store.
///
/// Persists object manifests as graph nodes on disk. Suitable for both
/// production use (with a durable local filesystem) and testing (with
/// [`tempfile::TempDir`]).
///
/// # Thread safety
///
/// The store is `Send + Sync` and may be shared across tasks via [`Arc`].
pub struct FsManifestStore {
    root: PathBuf,
    /// Monotonically increasing Lamport clock for CRDT ordering.
    seq: Arc<RwLock<u64>>,
}

impl FsManifestStore {
    /// Create (or open) a store rooted at `root`.
    ///
    /// The directory and its sub-directories are created lazily on first write.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            seq: Arc::new(RwLock::new(0)),
        }
    }

    // ── Internal path helpers ────────────────────────────────────────────────

    fn objects_dir(&self) -> PathBuf {
        self.root.join("objects")
    }

    fn versions_dir(&self) -> PathBuf {
        self.root.join("versions")
    }

    /// Stable file name for an object key: SHA-256 of the UTF-8 key bytes.
    fn object_filename(key: &str) -> String {
        let hash = hex::encode(Sha256::digest(key.as_bytes()));
        format!("{hash}.json")
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.objects_dir().join(Self::object_filename(key))
    }

    fn version_path(&self, node_id: &str) -> PathBuf {
        self.versions_dir().join(format!("{node_id}.json"))
    }

    async fn ensure_dirs(&self) -> Result<(), ObjectError> {
        for dir in [self.objects_dir(), self.versions_dir()] {
            tokio::fs::create_dir_all(&dir)
                .await
                .map_err(|e| ObjectError::Storage(format!("mkdir {}: {e}", dir.display())))?;
        }
        Ok(())
    }

    // ── CRDT helpers ─────────────────────────────────────────────────────────

    async fn next_seq(&self) -> u64 {
        let mut guard = self.seq.write().await;
        *guard += 1;
        *guard
    }

    // ── Low-level I/O ────────────────────────────────────────────────────────

    async fn read_node(&self, key: &str) -> Result<Option<ObjectNode>, ObjectError> {
        let path = self.object_path(key);
        if !path.exists() {
            return Ok(None);
        }
        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| ObjectError::Storage(format!("read {}: {e}", path.display())))?;
        let node: ObjectNode = serde_json::from_slice(&data)
            .map_err(|e| ObjectError::Storage(format!("deserialize node: {e}")))?;
        Ok(Some(node))
    }

    async fn write_node(&self, node: &ObjectNode) -> Result<(), ObjectError> {
        self.ensure_dirs().await?;
        let path = self.object_path(&node.manifest.key.0);
        let data = serde_json::to_vec_pretty(node)
            .map_err(|e| ObjectError::Storage(format!("serialize node: {e}")))?;
        tokio::fs::write(&path, data)
            .await
            .map_err(|e| ObjectError::Storage(format!("write {}: {e}", path.display())))?;
        Ok(())
    }

    async fn archive_current_node(&self, key: &str) -> Result<Option<String>, ObjectError> {
        let existing = match self.read_node(key).await? {
            Some(n) => n,
            None => return Ok(None),
        };
        let prev_id = existing.node_id.clone();
        self.ensure_dirs().await?;
        // Archive the current node to the versions directory before overwriting.
        let src = self.object_path(key);
        let dst = self.version_path(&prev_id);
        tokio::fs::copy(&src, &dst)
            .await
            .map_err(|e| ObjectError::Storage(format!("archive version {prev_id}: {e}")))?;
        Ok(Some(prev_id))
    }

    // ── Public graph API ──────────────────────────────────────────────────────

    /// Retrieve the full [`ObjectNode`] graph node for `key`, if it exists.
    ///
    /// This exposes the underlying graph representation, including the version
    /// history edge and chunk edges, which are not visible through the
    /// [`ManifestStorage`] trait.
    pub async fn get_node(&self, key: &ObjectKey) -> Result<Option<ObjectNode>, ObjectError> {
        self.read_node(&key.0).await
    }

    /// Retrieve a historical version node by its node UUID.
    ///
    /// Historical nodes are stored in `{root}/versions/` and are never mutated.
    pub async fn get_version(&self, node_id: &str) -> Result<Option<ObjectNode>, ObjectError> {
        let path = self.version_path(node_id);
        if !path.exists() {
            return Ok(None);
        }
        let data = tokio::fs::read(&path)
            .await
            .map_err(|e| ObjectError::Storage(format!("read version {node_id}: {e}")))?;
        let node: ObjectNode = serde_json::from_slice(&data)
            .map_err(|e| ObjectError::Storage(format!("deserialize version: {e}")))?;
        Ok(Some(node))
    }

    /// List the full version history for an object key, newest first.
    ///
    /// Traverses the `previous_version` edge chain from the current node,
    /// collecting each historical [`ObjectNode`] in order.
    pub async fn version_history(
        &self,
        key: &ObjectKey,
    ) -> Result<Vec<ObjectNode>, ObjectError> {
        let mut history = Vec::new();
        let mut prev_id = match self.read_node(&key.0).await? {
            Some(n) => n.previous_version,
            None => return Ok(history),
        };
        while let Some(id) = prev_id {
            match self.get_version(&id).await? {
                Some(node) => {
                    prev_id = node.previous_version.clone();
                    history.push(node);
                }
                None => break,
            }
        }
        Ok(history)
    }

    /// Apply an incoming [`ObjectNode`] from a remote peer (CRDT merge).
    ///
    /// Accepts the incoming node only when its `seq` is **strictly greater**
    /// than the local node's `seq`.  This implements last-write-wins semantics
    /// suitable for CRDT metadata replication across nodes.
    ///
    /// Returns `true` if the incoming node was accepted and written locally.
    pub async fn merge(&self, incoming: ObjectNode) -> Result<bool, ObjectError> {
        match self.read_node(&incoming.manifest.key.0).await? {
            Some(local) if local.seq >= incoming.seq => Ok(false),
            _ => {
                // Advance our local Lamport clock to be at least as high as
                // the incoming seq so future local writes are causally after it.
                {
                    let mut guard = self.seq.write().await;
                    if incoming.seq > *guard {
                        *guard = incoming.seq;
                    }
                }
                self.ensure_dirs().await?;
                self.write_node(&incoming).await?;
                Ok(true)
            }
        }
    }
}

// ── ManifestStorage impl ──────────────────────────────────────────────────────

#[async_trait::async_trait]
impl ManifestStorage for FsManifestStore {
    /// Store or replace the manifest for an object key.
    ///
    /// Before overwriting, the current graph node is archived to
    /// `{root}/versions/` so that version history remains traversable.
    async fn put(&self, manifest: Manifest) -> Result<(), ObjectError> {
        let prev_node_id = self.archive_current_node(&manifest.key.0).await?;

        let chunk_edges: Vec<String> = manifest
            .all_chunks()
            .into_iter()
            .map(|c| c.0.clone())
            .collect();

        let seq = self.next_seq().await;
        let node = ObjectNode {
            node_id: Uuid::new_v4().to_string(),
            seq,
            previous_version: prev_node_id,
            chunk_edges,
            manifest,
        };

        tracing::debug!(
            key = %node.manifest.key,
            seq = node.seq,
            node_id = %node.node_id,
            "manifest-db: writing object node"
        );

        self.write_node(&node).await
    }

    /// Get the manifest for an object key.
    async fn get(&self, key: &ObjectKey) -> Result<Manifest, ObjectError> {
        self.read_node(&key.0)
            .await?
            .map(|n| n.manifest)
            .ok_or_else(|| ObjectError::NotFound(key.0.clone()))
    }

    /// Delete the object node for `key`.
    ///
    /// Historical versions in `{root}/versions/` are intentionally retained so
    /// that the version history graph remains intact for auditing and recovery.
    async fn delete(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        let path = self.object_path(&key.0);
        if path.exists() {
            tokio::fs::remove_file(&path)
                .await
                .map_err(|e| ObjectError::Storage(format!("delete {}: {e}", path.display())))?;
        }
        Ok(())
    }

    /// List all current object keys, with an optional prefix filter.
    ///
    /// Scans the `{root}/objects/` directory (graph traversal over the object
    /// node set) and returns matching keys in lexicographic order.
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<ObjectKey>, ObjectError> {
        self.ensure_dirs().await?;
        let mut dir = tokio::fs::read_dir(self.objects_dir())
            .await
            .map_err(|e| ObjectError::Storage(format!("readdir objects: {e}")))?;

        let mut keys = Vec::new();
        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(|e| ObjectError::Storage(format!("readdir entry: {e}")))?
        {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let data = tokio::fs::read(&path)
                .await
                .map_err(|e| ObjectError::Storage(format!("read {}: {e}", path.display())))?;
            let node: ObjectNode = match serde_json::from_slice(&data) {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!("manifest-db: skipping corrupt node {}: {e}", path.display());
                    continue;
                }
            };
            let key_str = &node.manifest.key.0;
            let matches = prefix.map_or(true, |p| key_str.starts_with(p));
            if matches {
                keys.push(node.manifest.key);
            }
        }

        // Return keys in deterministic lexicographic order (graph traversal result).
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(keys)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
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
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn tagged_manifest(key: &str) -> Manifest {
        let mut m = test_manifest(key);
        m.tags.insert("project".into(), "summer".into());
        m.tags.insert("owner".into(), "alice".into());
        m
    }

    // ── Basic ManifestStorage operations ─────────────────────────────────────

    #[tokio::test]
    async fn put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        store.put(test_manifest("photos/a.jpg")).await.unwrap();
        let m = store.get(&"photos/a.jpg".into()).await.unwrap();
        assert_eq!(m.total_size, 1024);
        assert_eq!(m.etag, "abc123etag");
    }

    #[tokio::test]
    async fn get_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        let err = store.get(&"nope".into()).await.unwrap_err();
        assert!(matches!(err, ObjectError::NotFound(_)));
    }

    #[tokio::test]
    async fn delete_removes() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        store.put(test_manifest("x")).await.unwrap();
        store.delete(&"x".into()).await.unwrap();
        assert!(store.get(&"x".into()).await.is_err());
    }

    #[tokio::test]
    async fn delete_nonexistent_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        store.delete(&"missing".into()).await.unwrap();
    }

    // ── Persistence across restarts ───────────────────────────────────────────

    #[tokio::test]
    async fn persists_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = FsManifestStore::new(dir.path());
            store.put(test_manifest("backup/db.tar.gz")).await.unwrap();
        }
        // Reopen — simulate restart
        let store2 = FsManifestStore::new(dir.path());
        let m = store2.get(&"backup/db.tar.gz".into()).await.unwrap();
        assert_eq!(m.total_size, 1024);
        assert_eq!(m.etag, "abc123etag");
    }

    // ── Prefix listing ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn list_all() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        store.put(test_manifest("a/1")).await.unwrap();
        store.put(test_manifest("b/2")).await.unwrap();
        let all = store.list(None).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn list_with_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        store.put(test_manifest("photos/a")).await.unwrap();
        store.put(test_manifest("photos/b")).await.unwrap();
        store.put(test_manifest("docs/c")).await.unwrap();
        let photos = store.list(Some("photos/")).await.unwrap();
        assert_eq!(photos.len(), 2);
        assert!(photos.iter().all(|k| k.0.starts_with("photos/")));
    }

    #[tokio::test]
    async fn list_returns_lexicographic_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        store.put(test_manifest("z/3")).await.unwrap();
        store.put(test_manifest("a/1")).await.unwrap();
        store.put(test_manifest("m/2")).await.unwrap();
        let keys = store.list(None).await.unwrap();
        assert_eq!(keys[0].0, "a/1");
        assert_eq!(keys[1].0, "m/2");
        assert_eq!(keys[2].0, "z/3");
    }

    // ── Tag support ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn tags_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        store.put(tagged_manifest("tagged/obj")).await.unwrap();
        let m = store.get(&"tagged/obj".into()).await.unwrap();
        assert_eq!(m.tags.get("project").map(String::as_str), Some("summer"));
        assert_eq!(m.tags.get("owner").map(String::as_str), Some("alice"));
    }

    #[tokio::test]
    async fn tags_persist_across_restarts() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = FsManifestStore::new(dir.path());
            store.put(tagged_manifest("tagged/persist")).await.unwrap();
        }
        let store2 = FsManifestStore::new(dir.path());
        let m = store2.get(&"tagged/persist".into()).await.unwrap();
        assert_eq!(m.tags.get("project").map(String::as_str), Some("summer"));
    }

    // ── Version history ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn version_history_linked_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());

        // v1
        let mut v1 = test_manifest("obj/key");
        v1.etag = "etag-v1".into();
        store.put(v1).await.unwrap();

        // v2 overwrites v1
        let mut v2 = test_manifest("obj/key");
        v2.etag = "etag-v2".into();
        store.put(v2).await.unwrap();

        // v3 overwrites v2
        let mut v3 = test_manifest("obj/key");
        v3.etag = "etag-v3".into();
        store.put(v3).await.unwrap();

        // Current node should be v3
        let current = store.get_node(&"obj/key".into()).await.unwrap().unwrap();
        assert_eq!(current.manifest.etag, "etag-v3");
        assert!(current.previous_version.is_some());

        // Version history should contain v2 then v1 (newest first)
        let history = store.version_history(&"obj/key".into()).await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].manifest.etag, "etag-v2");
        assert_eq!(history[1].manifest.etag, "etag-v1");
    }

    #[tokio::test]
    async fn first_write_has_no_previous_version() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());
        store.put(test_manifest("fresh/object")).await.unwrap();
        let node = store.get_node(&"fresh/object".into()).await.unwrap().unwrap();
        assert!(node.previous_version.is_none());
    }

    #[tokio::test]
    async fn version_history_persists_after_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());

        let mut v1 = test_manifest("del/obj");
        v1.etag = "etag-v1".into();
        store.put(v1).await.unwrap();
        let node = store.get_node(&"del/obj".into()).await.unwrap().unwrap();
        let v1_node_id = node.node_id.clone();

        let mut v2 = test_manifest("del/obj");
        v2.etag = "etag-v2".into();
        store.put(v2).await.unwrap();

        // Delete current object node
        store.delete(&"del/obj".into()).await.unwrap();

        // v1 historical node still accessible via get_version
        let archived = store.get_version(&v1_node_id).await.unwrap();
        assert!(archived.is_some());
        assert_eq!(archived.unwrap().manifest.etag, "etag-v1");
    }

    // ── CRDT merge ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn merge_accepts_higher_seq() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());

        store.put(test_manifest("crdt/obj")).await.unwrap();
        let local_node = store.get_node(&"crdt/obj".into()).await.unwrap().unwrap();
        assert_eq!(local_node.seq, 1);

        // Incoming node with higher seq should win
        let mut incoming = local_node.clone();
        incoming.seq = 99;
        incoming.manifest.etag = "from-remote".into();
        incoming.node_id = Uuid::new_v4().to_string();

        let accepted = store.merge(incoming).await.unwrap();
        assert!(accepted);
        let m = store.get(&"crdt/obj".into()).await.unwrap();
        assert_eq!(m.etag, "from-remote");
    }

    #[tokio::test]
    async fn merge_rejects_lower_seq() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());

        store.put(test_manifest("crdt/obj")).await.unwrap();
        let local_node = store.get_node(&"crdt/obj".into()).await.unwrap().unwrap();

        // Incoming node with lower seq should be rejected
        let mut stale = local_node.clone();
        stale.seq = 0;
        stale.manifest.etag = "stale-remote".into();

        let accepted = store.merge(stale).await.unwrap();
        assert!(!accepted);
        let m = store.get(&"crdt/obj".into()).await.unwrap();
        assert_eq!(m.etag, "abc123etag"); // local unchanged
    }

    #[tokio::test]
    async fn merge_rejects_equal_seq() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());

        store.put(test_manifest("crdt/tie")).await.unwrap();
        let local_node = store.get_node(&"crdt/tie".into()).await.unwrap().unwrap();

        // Tie-breaking: equal seq → local wins
        let mut tie = local_node.clone();
        tie.manifest.etag = "remote-tie".into();

        let accepted = store.merge(tie).await.unwrap();
        assert!(!accepted);
        let m = store.get(&"crdt/tie".into()).await.unwrap();
        assert_eq!(m.etag, "abc123etag");
    }

    // ── Chunk edges ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn chunk_edges_stored_in_node() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsManifestStore::new(dir.path());

        let mut m = test_manifest("edges/obj");
        m.parts[0].chunks = vec![ChunkId("aaa".into()), ChunkId("bbb".into())];
        store.put(m).await.unwrap();

        let node = store.get_node(&"edges/obj".into()).await.unwrap().unwrap();
        assert_eq!(node.chunk_edges, vec!["aaa", "bbb"]);
    }
}

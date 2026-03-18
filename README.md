# Plures Object + Stream Platform

**S3-compatible object storage + streaming platform powered by PluresDB.**

> Content-addressed chunks • Graph-native metadata • Hyperswarm P2P replication

## Architecture

```
┌─────────────────────────────────────────────────┐
│  S3 API Gateway                                 │
│  PutObject · GetObject · DeleteObject · List    │
├─────────────────────────────────────────────────┤
│  Object Service (plures-object-store)           │
│  Orchestrates chunks + manifests                │
├───────────────────┬─────────────────────────────┤
│  ChunkStore       │  Manifest Store             │
│  Content-addressed│  Object → chunk mapping     │
│  SHA-256 dedup    │  Multipart support          │
├───────────────────┴─────────────────────────────┤
│  PluresDB (metadata plane)                      │
│  CRDT graph · Vector search · Hyperswarm sync   │
├─────────────────────────────────────────────────┤
│  Stream Engine (plures-stream)                  │
│  Events · Replication · Workflows               │
└─────────────────────────────────────────────────┘
```

## Crates

| Crate | Purpose |
|-------|---------|
| `plures-object-core` | Shared types, traits, error types |
| `plures-chunkstore` | Content-addressed blob storage (fs + memory) |
| `plures-manifest` | Object-to-chunk manifest storage |
| `plures-object-store` | Object service (PutObject, GetObject, etc.) |
| `plures-stream` | Event streaming engine |

## On-Disk Layout

```
data/
├── chunks/          # Content-addressed blobs, sharded by hash prefix
│   ├── a1/
│   │   └── a1b2c3...
│   └── ff/
│       └── ffab01...
├── manifests/       # Object → chunk mappings (PluresDB backed)
├── staging/         # In-progress multipart uploads
└── gc/              # Garbage collection metadata
```

## Quick Start

```rust
use std::sync::Arc;
use plures_object_store::ObjectService;
use plures_chunkstore::MemChunkStore;
use plures_manifest::MemManifestStore;

#[tokio::main]
async fn main() {
    let svc = ObjectService::new(
        Arc::new(MemChunkStore::new()),
        Arc::new(MemManifestStore::new()),
    );

    // PutObject
    let meta = svc.put_object(
        "photos/sunset.jpg",
        bytes::Bytes::from_static(b"image data..."),
        Some("image/jpeg".into()),
    ).await.unwrap();

    println!("Stored: {} ({} bytes, etag: {})", meta.key, meta.size, meta.etag);

    // GetObject
    let (_, data) = svc.get_object(&"photos/sunset.jpg".into()).await.unwrap();
    println!("Retrieved {} bytes", data.len());

    // ListObjects
    let objects = svc.list_objects(Some("photos/")).await.unwrap();
    println!("Found {} objects", objects.len());
}
```

## Key Features

- **Content-addressed deduplication** — identical data stored once regardless of how many objects reference it
- **Chunked storage** — large objects split into configurable chunks (default 8 MiB)
- **S3-compatible API surface** — PutObject, GetObject, DeleteObject, ListObjects, HeadObject
- **Event streaming** — every operation emits events for replication, workflows, and audit
- **Pluggable backends** — trait-based storage allows memory, filesystem, or PluresDB backends

## Milestones

1. ✅ Core storage + metadata (this commit)
2. ⬜ Multipart upload support
3. ⬜ S3 HTTP API layer (axum)
4. ⬜ PluresDB metadata backend (replace MemManifestStore)
5. ⬜ Hyperswarm replication via Stream Engine
6. ⬜ Garbage collection for orphaned chunks

## Part of the Plures Ecosystem

| Package | Role |
|---------|------|
| [PluresDB](https://github.com/plures/pluresdb) | Graph database + vector search + Hyperswarm |
| [Unum](https://github.com/plures/unum) | Reactive state bindings (Svelte 5 ↔ PluresDB) |
| [Chronos](https://github.com/plures/chronos) | Graph-native state chronicle |
| **Plures Object** | S3-compatible object storage + streaming |
| [Pares Agens](https://github.com/plures/pares-agens) | AI agent framework |
| [Design Dojo](https://github.com/plures/design-dojo) | UI component library |
| [Plures Vault](https://github.com/plures/plures-vault) | Encrypted secret storage |

## License

AGPL-3.0-or-later

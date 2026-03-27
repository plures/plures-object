# plures-object Roadmap

## Role in Plures Ecosystem
plures-object is the S3-compatible object storage and streaming layer for PluresDB. It enables content-addressed storage with CRDT-backed metadata and P2P replication, forming the backbone for large asset handling across Plures products.

## Current State
Core crates exist for chunk storage, manifest storage, object service logic, and a stream engine. The README outlines an S3-compatible API surface, but HTTP gateway, multipart upload, and PluresDB-backed metadata are not fully wired yet. Replication and access control are still early.

## Milestones

### Near-term (Q2 2026)
- Implement multipart upload support with staging + completion flow.
- Add S3 HTTP API gateway (axum) with auth hooks.
- Wire PluresDB manifest backend to replace in-memory store.
- Add streaming events for all object operations.
- Expand tests for chunk integrity and manifest correctness.

### Mid-term (Q3–Q4 2026)
- Add Hyperswarm-based replication with conflict resolution.
- Implement access control policies and per-bucket keys.
- Add lifecycle policies (retention, archive, delete).
- Optimize chunking strategies and compression options.
- Provide SDK bindings for Rust + TypeScript.

### Long-term
- Multi-region P2P federation with audit trails.
- Enterprise-grade S3 compatibility suite (AWS parity tests).
- Integrated object analytics and queryable metadata.
- Production-ready garbage collection + repair tooling.

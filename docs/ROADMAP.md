# plures-object Roadmap

## Role in OASIS
plures-object is OASIS’s content distribution and storage layer. Privacy‑preserving commerce depends on durable, content‑addressed objects that can be replicated peer‑to‑peer with verifiable integrity. This repo provides the S3‑compatible object API, chunked storage, and streaming events that let OASIS publish, move, and verify assets across nodes without centralized trust.

## Current State
Phase 1 is complete. The S3‑compatible API surface is fully implemented with multipart upload, HTTP gateway (axum), PluresDB‑backed manifest storage with CRDT version history, and a streaming event bus. All core crates compile cleanly and pass 70+ tests covering chunk integrity, ETag correctness, multipart lifecycle, and manifest consistency.

## Milestones

### Phase 1 — Ship the S3 surface ✅ (Completed Q3 2026)
- ✅ Implement multipart upload support with staging + completion flow.
- ✅ Add S3 HTTP API gateway (axum) with XML error responses.
- ✅ Wire PluresDB manifest backend (graph‑native, CRDT, version history).
- ✅ Emit streaming events for object operations (create, delete, chunk, replication).
- ✅ Expand tests for chunk integrity, ETag correctness, and manifest consistency.
- ✅ Add CLI (`plures-object serve`) for running the gateway.

### Phase 2 — P2P replication + policy (Q3–Q4 2026)
- Add Hyperswarm‑based replication with conflict resolution.
- Implement access control policies and per‑bucket keys.
- Add lifecycle policies (retention, archive, delete).
- Implement garbage collection for orphaned chunks.
- Optimize chunking strategies and compression options.
- Provide SDK bindings for Rust + TypeScript.

### Phase 3 — OASIS‑grade operations (2027+)
- Multi‑region P2P federation with audit trails.
- Enterprise‑grade S3 compatibility suite (AWS parity tests).
- Integrated object analytics and queryable metadata.
- Production‑ready garbage collection + repair tooling.

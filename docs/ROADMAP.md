# plures-object Roadmap

## Role in OASIS
plures-object is OASIS’s content distribution and storage layer. Privacy‑preserving commerce depends on durable, content‑addressed objects that can be replicated peer‑to‑peer with verifiable integrity. This repo provides the S3‑compatible object API, chunked storage, and streaming events that let OASIS publish, move, and verify assets across nodes without centralized trust.

## Current State
Core crates are in place for chunk storage, manifest storage, object service logic, and a stream engine. The README describes an S3‑compatible API surface and PluresDB‑backed metadata, but the HTTP gateway, multipart upload flow, and PluresDB manifest backend are not fully wired yet. Replication, access control, and lifecycle management remain early.

## Milestones

### Phase 1 — Ship the S3 surface (Now → Q2 2026)
- Implement multipart upload support with staging + completion flow.
- Add S3 HTTP API gateway (axum) with auth hooks.
- Wire PluresDB manifest backend to replace in‑memory store.
- Emit streaming events for all object operations.
- Expand tests for chunk integrity, ETag correctness, and manifest consistency.

### Phase 2 — P2P replication + policy (Q3–Q4 2026)
- Add Hyperswarm‑based replication with conflict resolution.
- Implement access control policies and per‑bucket keys.
- Add lifecycle policies (retention, archive, delete).
- Optimize chunking strategies and compression options.
- Provide SDK bindings for Rust + TypeScript.

### Phase 3 — OASIS‑grade operations (2027+)
- Multi‑region P2P federation with audit trails.
- Enterprise‑grade S3 compatibility suite (AWS parity tests).
- Integrated object analytics and queryable metadata.
- Production‑ready garbage collection + repair tooling.

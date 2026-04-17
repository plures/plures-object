## [0.5.1] — 2026-04-17

- fix(lifecycle): label-based retry counter + CI fix priority (f035ca8)

## [0.5.0] — 2026-04-08

- feat: PluresDB metadata backend — graph-native object metadata (#7) (94971d4)

## [0.4.0] — 2026-04-08

- feat: S3-compatible HTTP API via axum — GET/PUT/DELETE/HEAD/LIST (#6) (87614d5)

## [0.3.0] — 2026-04-08

- feat: multipart upload support — chunked upload for large objects (#5) (1090f03)
- ci: inline lifecycle workflow — fix schedule failures (79bf15b)
- ci: tech-doc-writer triggers on minor prerelease only [actions-optimization] (c257edf)
- ci: add concurrency group to copilot-pr-lifecycle [actions-optimization] (969ec3c)
- ci: centralize lifecycle — event-driven with schedule guard (7750ac7)

## [0.2.5] — 2026-04-01

- fix(lifecycle): v9.2 — process all PRs per tick (return→continue), widen bot filter (4f331b0)

## [0.2.4] — 2026-04-01

- fix(lifecycle): change return→continue so all PRs process in one tick (fbf1e3e)

## [0.2.3] — 2026-03-31

- fix(lifecycle): v9.1 — fix QA dispatch (client_payload as JSON object) (3d8818b)

## [0.2.2] — 2026-03-31

- fix(lifecycle): rewrite v9 — apply suggestions, merge, no nudges (ccba4ac)
- chore: standardize license to MIT (4c0fa37)
- chore: standardize copilot-pr-lifecycle.yml to canonical version (b2329fd)

## [0.2.1] — 2026-03-28

- fix: add packages:write + id-token:write to release workflow (f7c4235)
- docs: add ROADMAP.md (abb133a)

# Changelog

## [0.2.0] — 2026-03-27

- Merge pull request #1 from plures/chore/org-standards (d7031a3)
- Update .github/workflows/copilot-pr-lifecycle.yml (fca61e9)
- chore: add Reusable release pipeline (17781f9)
- chore: add Auto-create doc issues on PR merge (87fa40f)
- chore: add Copilot PR auto-merge lifecycle (e607e1d)
- chore: add Copilot coding instructions (fadaeb2)
- feat: initial plures-object — S3-compatible object storage + streaming (3d76e8e)


## [0.10.0] — 2026-08-09

- Merge pull request #16 from plures/copilot/emit-streaming-events-for-operations (182ad7c)
- Potential fix for pull request finding (93a3a4a)
- fix: capture delete timestamp before the operation (8707f04)
- feat: emit streaming events for all object operations (36fa5cc)
- Initial plan (4063578)
- Merge pull request #14 from plures/chore/dependabot-auto-merge-workflow-call (7a1b1d6)
- chore: delegate dependabot-auto-merge.yml to plures/.github reusable template (50a8a0f)

## [0.9.0] — 2026-07-24

- Merge pull request #13 from plures/release-trigger-autobump (5400b71)
- Potential fix for pull request finding (0a2b9e6)
- ci(release): trigger release pipeline on merge to main (1401b96)
- ci: migrate Tech Doc Writer to shared reusable (c02717e)
- fix(ci): repair tech-doc-writer YAML indentation / remove empty workflow (5cb35ce)
- ci: add security-aware Dependabot auto-merge workflow (org backfill) (bad693a)
- ci: change release trigger from push-to-main to tag-only (f961e6f)
- refactor: replace inline lifecycle with reusable workflow call (a670528)
- feat: add object-cli binary crate — standalone S3-compatible server (27fdea5)
- fix: suppress ci-feedback issue spam (24h dedup window) (ba89353)
- feat: wire PluresDB manifest backend into ObjectService (#10) (59c5009)
- docs: refresh ROADMAP.md with OASIS strategic alignment (2ac305d)
- docs: update copilot-instructions with praxis, design-dojo, automation rules (eaddef0)
- feat(release): add target_version input for milestone-driven releases (0ff8406)
- feat(lifecycle): milestone-close triggers roadmap-aware release (769bcea)
- docs: update copilot-instructions with Plures stack architecture (95937d6)
- docs: update copilot-instructions with Plures stack architecture (9c703b4)

## [0.7.0] — 2026-04-18

- feat(lifecycle v12): auto-release when milestone completes (9264629)

## [0.6.0] — 2026-04-18

- feat(lifecycle v11): smart CI failure handling — infra vs code (28861a0)

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


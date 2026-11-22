# AI Development Handoff

## Cycle Objective
Implement terminal asynchronous writer-failure routing through channel-owned ErrorStreams, then remove the obsolete stream registry and `Get()` API.

## Current Repository State
- **Persisted lifecycle phase:** IMPLEMENT (matches `cycle-index.json`).
- Repository: `gunesis-game/pulsyflux-stream`
- Branch: `main`
- This cycle is in progress. The latest code checkpoint includes private async persistence, shared writer ownership, and Publish integration. Current ErrorStream and registry-removal changes are prepared for verification; independently confirm source and CI before relying on them.

## Completed
- Prior milestone documented the private bounded async writer, process-wide shared writer with owner count, channel writer ownership, redacted Publish persistence, zero-subscriber persistence, slow-subscriber preflight, and concurrent-producer persistence test.
- Prior checkpoint reported Linux and Windows CI passing race-enabled tests/coverage, vet, build, formatting, and artifact upload. Treat that as historical checkpoint evidence and verify current status as needed.

## Decisions and Constraints
- Follow the approved ErrorStream requirements and public/private API constraints in `steering.md`.
- ErrorStream is channel-owned and exposed only through `Errors(bufferSize)`; error notifications are not persisted.
- Preserve the underlying failure cause for `errors.Is`/`errors.As`; route one terminal notification per affected owner with reserved capacity.
- No stream registry/cache as a replacement for persisted channel/stream records; remove `Get()` and retrieval-only map.
- Disk storage remains private; no release/version bump; replay remains deferred.

## Verification
- Added lazy channel-owned `Errors(bufferSize)` returning a system-only `Stream[ErrorStream, StreamWriteErrorEvent]`; it retains and routes the first terminal writer cause once, bypasses persistence, and reserves one subscriber slot.
- Changed channel Close to keep ErrorStream alive through writer ownership release/failure routing, then close it. Normal stream subscriptions observe the channel close signal without a retained stream registry.
- Removed the stream lookup map and `Get()`; `New` now returns independent caller-owned stream instances, including when event types repeat.
- Updated stream tests for the new ownership model and added ErrorStream cause, lazy-after-failure, system-only, and close-order tests.
- Reconciled `docs/storage_disk.md` and `docs/storage_integration_contract.md` with current implementation.
- These changes are not yet committed or CI-verified. Local Go execution is unavailable in this assistant environment; after a reviewed checkpoint, use exact-commit GitHub Actions CI and inspect all jobs/logs/artifacts.
- Prior 82.4% coverage is historical only; compare Linux/Windows aggregate and per-function coverage from the same new CI run.

## Outstanding Risks
- Error notification routing must not deadlock when writer failure callbacks occur while Publish holds synchronization.
- Close ordering must preserve access to ErrorStreams until ownership release and any failure notifications have been routed.
- Ensure terminal-failure fan-out deduplicates owners and covers affected queued records.
- Validate docs against implementation.

## Current Phase Checkpoint
- **Phase:** IMPLEMENT
- **Exit condition:** A coherent ErrorStream/failure-routing implementation with focused tests is ready for VERIFY.
- **Backward transition rule:** Return to DEFINE only if source inspection or tests demonstrate a real contradiction with the approved architecture or a missing requirement. Ordinary implementation choices are resolved autonomously.

## Next Task
Add deterministic async-writer append/flush failure injection, then test first-cause propagation, owner fan-out/deduplication, queue saturation/backpressure, Publish-after-failure, and Close/failure races. Review the implementation and run exact-commit Linux/Windows CI; fix any failures before transitioning to VERIFY.

## Acceptance Criteria
See the cycle `plan/PLAN.md`. Do not mark complete until all criteria and required CI/coverage verification are satisfied.

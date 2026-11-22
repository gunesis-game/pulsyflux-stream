# Cycle Plan

## Objective
Implement terminal asynchronous persistence-failure propagation through channel-owned ErrorStreams, then remove the obsolete stream registry and `Get()` API in a coherent lifecycle change.

## Scope
- Implement the approved channel-owned `Errors(bufferSize)` access boundary and typed `StreamWriteErrorEvent`.
- Deliver terminal writer failures to affected channel owners exactly once, bypassing persistence, preserving the underlying cause for `errors.Is`/`errors.As`, and using reserved notification capacity.
- Avoid deadlocks between writer callbacks and Publish synchronization.
- Remove the stream map/registry and `Get()` API where they are only used for retrieval; preserve subscriber closure/lifecycle behavior without replacing the registry with another lookup cache.
- Add failure-injection and lifecycle/concurrency tests.
- Reconcile `docs/storage_disk.md` and related contracts with actual implementation.

## Non-goals
- No public disk-storage API or storage injection.
- No schema/catalog/fingerprint framework.
- No replay implementation in this cycle unless required to resolve a blocking defect in the scoped work.
- No release/version bump.

## Constraints and design context
Follow the approved architecture and ErrorStream requirements in `steering.md`. Error notifications are system events and must never be persisted. The shared writer failure fan-out is owner-based and deduplicated, not registry-based. ErrorStream is channel-owned and exposed through `Errors(bufferSize)`; subscriptions use the normal Subscribe interface. One terminal failure notification per affected channel, with reserved delivery capacity. Preserve cause wrapping. Channel close must not prematurely close ErrorStream before ownership release and failure routing.

## Persisted execution phase
- **Current phase:** RECONCILE
- **Why:** Implementation and verification are complete for the scoped cycle. Deterministic append/flush failure injection, multi-owner fan-out, deduplication, queue backpressure, blocked Publish, reserved capacity, and close-order tests pass on Linux and Windows. Both CI jobs pass all required checks; coverage artifacts are identical at 86.0% aggregate statement coverage and per-function coverage.
- **Phase exit:** Audit final implementation against the plan, architecture, storage docs, and handoff. Resolve any mismatch before marking the cycle complete.

## Ordered work
1. Re-validate the current writer, channel, stream, event, and storage tests against the current repository.
2. Implement terminal writer-failure routing and channel-owned ErrorStream lifecycle.
3. Add tests for terminal failure, reserved error capacity, cause preservation, owner fan-out/deduplication, publish-after-failure, close ordering, and deadlock/race safety.
4. Remove `Get()` and map-based stream registry; maintain lifecycle closure semantics and update affected tests.
5. Reconcile documentation, especially `docs/storage_disk.md`, with implementation.
6. Review the complete diff and run required CI checks (Linux and Windows); repair failures.
7. Compare aggregate and per-function coverage reports from the same CI run and update the handoff.

## Acceptance criteria
- ErrorStream is channel-owned, lazily accessible through `Errors(bufferSize)`, with one internal stream per EventChannel.
- Terminal failure is delivered once to affected owners, including relevant pending queue owners; notifications bypass persistence and preserve the original error cause.
- Reserved error-delivery capacity prevents normal business-event buffer saturation from consuming the terminal notification slot.
- Failure routing cannot deadlock with Publish or channel Close.
- Publish after terminal failure returns an error wrapping the authoritative terminal cause and does not claim persistence.
- `Get()` and the retrieval-only map registry are removed without introducing a replacement lookup registry.
- Subscribe/cancel/Close behavior remains race-safe and covered.
- Storage documentation and integration contracts match code.
- Required Linux and Windows CI passes for formatting, race-enabled tests/coverage, vet, build, and artifact upload; coverage comparison is completed.
- Cycle status is marked complete only after these criteria are verified.

## Progress
- [x] Step 1 — revalidate current source and tests against the persisted IMPLEMENT checkpoint.
- [x] Step 2 (initial implementation) — add lazy channel-owned ErrorStream, terminal-cause retention/routing, system-only bypass, reserved notification capacity, and close-after-writer-release ordering.
- [x] Step 4 (initial implementation) — remove the retrieval-only stream map and `Get()`; streams are caller-owned and channel shutdown is signalled without a lookup registry.
- [x] Step 5 — reconcile storage and integration docs with the integrated implementation and completed verification.
- [ ] Remaining — add writer failure injection, reserved-capacity and queue saturation/backpressure stress tests; review concurrency/lifecycle edges; run exact-commit Linux/Windows CI and compare coverage.

Progress is updated as work is completed. The registry `current_phase` is authoritative; this section records the operational checkpoint.

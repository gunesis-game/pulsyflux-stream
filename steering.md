# Steering: pulsyflux-stream

## Purpose
Maintain a small, typed, concurrency-safe Go event-stream library. Keep the live-stream core lightweight. Event payload validation/redaction belongs to Event; disk persistence is an internal subsystem.

## Project status — NOT LIVE, NOT PRODUCTION

**This Go project is not live, not released, and not in production. It is still under active architecture and implementation development. There are no production compatibility obligations to preserve.**

This status is authoritative for all work in this repository:
- Do not assume there are users, deployments, production data, released consumers, or a stable public API.
- Do not preserve legacy code, APIs, storage layouts, or abstractions merely for backward compatibility. Change or delete them when the new architecture requires it.
- Breaking API and on-disk format changes are acceptable while the project is pre-production. Prefer the correct architecture over compatibility with earlier development iterations.
- **Do not bump the Go module version, release version, or disk-format version just because code or architecture changed.** Version changes require an explicit reason and must be tied to an actual release/compatibility/migration requirement, not routine development.
- A disk-format version marker (for example, an internal format revision) describes record encoding compatibility only. It does not mean the project is released, live, production-ready, or that the format is frozen. Do not describe an in-development format as a production/current stable version.
- Do not add migration frameworks, compatibility shims, deprecation periods, or version-transition machinery for unreleased development formats unless explicitly required.
- Never initiate a release or production rollout unless the user explicitly asks.

## Agreed architecture and identity
- **Identity is type-derived, not UUID-derived.** `EventChannel[TStream]` identity is `EventChannel[TStream.name]` (e.g. `EventChannel[AuthStream]`). `Stream[TStream,TEvent]` identity is `Stream[TStream.name,TEvent]` (e.g. `Stream[AuthStream,AuthTokenValidEvent]`). Use a stable canonical type-name representation appropriate to Go types; do not rely on ambiguous bare names if package-qualified identity is needed to avoid collisions.
- `EventChannel` and `Stream` do not own or generate UUIDs. Do not add UUID recovery, identity catalogs, or deterministic UUID derivation.
- Event UUIDs remain for individual event identity unless the user explicitly changes that requirement.
- **ABSOLUTE PUBLIC-API RULE — DISK STORAGE MUST NEVER BE PUBLIC.** Disk storage is an internal implementation detail, not a consumer-facing dependency. Never expose a disk store type, constructor, interface, parameter, field, option, configuration object, or storage handle in the public API. Consumers must never construct, receive, configure, or pass a disk store. Do not suggest or implement `NewDiskStore(...)` or `NewEventChannel[TStream](store)`. This rule is explicit and overrides any earlier wording that could imply otherwise.
- **Event-only persistence:** event records are the only persisted record type. There are no channel records, stream records, channel catalog, or separately persisted channel entities. Each event record carries its type-derived channel and stream identities alongside event metadata and the redacted payload. A channel or stream with no events has no persisted representation.
- **Private storage lifecycle:** the library creates/opens and owns its private disk storage internally. Channel/stream construction must not load, create, or require a separate channel/stream record. Event records are discovered by scanning/filtering their embedded type-derived identities. Do not invent record types to represent empty channels or streams.
- The public channel constructor must not accept storage arguments. Its public signature and error behavior must be designed around internally managed storage; do not preserve the old parameterless constructor requirement or `channel.NewStream()` lifecycle if they conflict with the corrected architecture.
- Remove the `Get()` API and map-based stream registry if they exist only to retrieve already-loaded streams. Do not add an in-memory registry/cache as a substitute for persisted channel/stream records: no such records exist. Runtime stream objects may own subscriber-delivery state, but persisted event records remain the only storage authority.
- Disk-store construction, location, initialization, handles, and lifecycle remain entirely private. The agreed private storage root is a `pulsyflux-stream-data` subdirectory of the process's current working directory, resolved to an absolute path when internal storage is initialized. Constructor error propagation must be explicit without exposing storage types or handles.
- No schema fingerprint framework. Delete `schema.go` and its fingerprint composition/base/effective machinery. Do not replace it with another schema/catalog subsystem.
- Keep public generic relationship: `EventChannel[TStream]`, `Stream[TStream,TEvent]`, `Event[TEvent]`. One channel family can contain multiple streams with different event types.
- The event/stream/channel human-readable identity strings are type-derived. Channel and stream identities are metadata on event records used to associate events with their typed channel/stream; they do not imply separate persisted channel or stream records and are not instance IDs.

## Ownership
- **Event (`event.go`):** typed payload, creation timestamp, redaction policy, payload assignment/validation, serialization/deserialization and event lifecycle. Move any Event-specific functionality currently in `schema.go` into `event.go`; do not leave a schema file behind.
- **Stream (`stream.go`):** concurrency-safe typed delivery of `Event[TEvent]` values to subscribers. Publish sends live unredacted events; persistence redacts before writing.
- **Channel (`channel.go`):** typed channel identity and private storage lifecycle. There is no persisted channel entity to load or create. Channel identity is represented only in metadata on its event records. No channel UUID and no map-based stream lookup registry.
- **Storage (`storage_disk.go` and related internal files):** append-only disk records, framing/checksum, durability, scanning/recovery and rebuildable metadata. Persist event UUID, channel/stream type-derived identities, sequence, event-creation timestamp, and redacted payload as required by the actual format. Do not persist schema fingerprints or duplicate type identity/timestamp inside payload. **Disk storage is permanently private: no public storage API, public store type, public constructor, public injection point, or consumer configuration.**
- **Replay:** dispatches historical records through existing typed stream/channel abstractions; replay is not recorded again by default.

## Redaction and payload invariants
- Every Event has a RedactionPolicy; empty policy is explicit opt-out. Policy is copied and immutable.
- Validate exclusion paths at construction and again before payload assignment. Reject blank, duplicate, malformed, or unresolved paths.
- Redact a serialized copy; never mutate live payload. Deserialization cannot recover excluded values.
- Supported traversal: structs, pointers, slices/arrays of structs. Maps and embedded-field flattening are out of scope until deliberately designed and tested.
- No Event envelope. Payload is redacted data directly; event ID, sequence, and timestamp belong to record metadata as appropriate.
- Do not add public Validate/Serialize/Deserialize/TypeName methods or expose event payload/time/type metadata as public fields.

## Stream behavior
- Stream construction uses the channel's private storage ownership/lifecycle, but there is no persisted stream record to load or create. Event records carry stream identity. Do not implement a stream-record operation, in-memory lookup registry, or duplicate-registration map as a substitute for persistence.
- Each stream delivers only `Event[TEvent]` to subscribers. Different `TEvent` streams coexist under one `TStream` channel.
- Publish is atomic across subscribers: if any subscriber is full, return `ErrSlowSubscriber` and deliver to none. Publish with zero subscribers succeeds.
- Preserve concurrency safety for Subscribe, Publish, cancellation, unsubscribe and Close.

- **Agreed asynchronous persistence behavior (2026-11-22):** individual Publish calls must not write to disk. Publish validates/preflights, submits a redacted event record to a bounded private queue, then delivers live after successful queue admission. A private writer batches multiple records and appends when an internal size threshold is reached or a short maximum delay expires. If the queue is full, Publish applies backpressure by waiting for capacity; memory must not grow without bound. Successful Publish means queue acceptance and live delivery, not disk persistence or crash durability; a process crash may lose queued or unflushed records. Queue admission order and disk append order must be preserved. Asynchronous disk failures can occur after Publish returns and live delivery occurs. The approved failure-reporting and writer behavior are defined in **Approved writer lifecycle and error handling** below. Exact batch thresholds are private implementation details.

## Disk storage principles
- Keep storage taxonomy explicit: lifecycle, write, read/scan, lookup, replay, maintenance, metadata.
- Prefer append-only segmented files. Make framing, checksum, segment rollover, sync policy and recovery boundaries explicit. Use an internal format revision only when needed to interpret incompatible record encodings; it is not a release version and does not imply production stability.
- Flush (application buffering) and Sync (OS/filesystem durability) are distinct; document durability windows.
- Storage handles redacted payload bytes and I/O; it does not own Go payload types or redaction.
- Begin with sequential scan and rebuildable/minimal indexes. No SQL/distributed infrastructure absent concrete requirements.
- Avoid parallel public event/stream/channel/record models unless a real boundary needs them.

## Approved writer lifecycle and error handling (2026-11-22)

- **Shared writer lifecycle approved:** use one process-wide private writer shared by all active EventChannels. The manager tracks active-owner count only; it does not maintain a channel registry. Closing an individual EventChannel releases its ownership but does not stop the writer while other channels remain active. Closing the final active EventChannel initiates orderly shutdown: stop queue admission, drain accepted queued records, flush, and close storage. A subsequently created EventChannel may initialize a fresh writer. Shutdown does not imply OS-level Sync unless an explicit internal durability operation is defined; the final Close returns the drain/flush/close errors. Channel ErrorStreams remain open until shutdown/failure notifications are routed, then close with other streams.
- **Terminal storage failure approved:** an unrecoverable asynchronous storage failure transitions the shared writer to a terminal failed state. Preserve the first underlying storage error as the authoritative cause. Stop accepting persisted events; subsequent Publish calls that require persistence return an error wrapping that cause. Do not silently discard queued events or report them as persisted.
- **System error event:** the library owns the `StreamWriteErrorEvent` type and creates/publishes it when the writer encounters a terminal storage failure. It is a system notification and must never be persisted, avoiding recursive write failures. Its error value must preserve the underlying cause for `errors.Is`/`errors.As`.
- **Normal generic API only:** consumers use the existing public generic API and constructor; there is no separate ErrorChannel implementation, special registration API, or separate subscription mechanism. Error events use the normal `Stream[ErrorStream, TErrorEvent]` machinery and the same `New[ErrorStream, TErrorEvent](errorChannel, bufferSize)` construction pattern for the error stream family.
- **One internal ErrorStream per EventChannel:** constructing each `EventChannel[TStream]` automatically creates exactly one private `ErrorStream` associated with that EventChannel. All error event streams for that EventChannel share this ErrorStream. It is not one ErrorStream per normal Stream, and there is no global error-channel registry or consumer registration/unregistration mechanism.
- The internal ErrorStream is library-owned. The library publishes system error events through an internal publishing path; consumers subscribe through the normal stream API. Error publishing bypasses disk persistence but retains normal typed subscriber delivery semantics.
- **Reserved error-delivery capacity approved:** each error subscriber has reserved capacity for the system error notification, so a full normal business-event subscriber buffer cannot consume or block that reserved capacity. Ensure notification delivery itself does not depend on the failed disk writer.
- Because the writer is shared process-wide, a fatal writer failure is fanned out to the distinct EventChannel owners represented in the failed batch and pending queue. Queue items carry a private owner reference used only for error routing; owners are deduplicated per failure. This is not a global channel registry. A channel with a later persisted Publish after terminal failure receives the terminal error through that Publish path and its channel-local error stream.
- Error-stream lifetime is scoped to its owning EventChannel. Close remains idempotent; channel Close must keep internal error streams available until writer ownership release and any failure notifications are routed, then close all streams/subscriptions. Final-owner Close returns drain/flush/close errors; no explicit Sync guarantee is made.

## Development rules
- Work directly on `main`; do not create branches unless explicitly requested. Use GitHub MCP for repository operations; cloning is unnecessary.
- Before editing, check current main HEAD, recent commits, relevant files/tests/docs and CI. Do not trust stale handoffs.
- Implement coherent changes, commit directly to main, and review the diff.
- After each code push, identify CI for that exact commit SHA. Wait for completion and inspect conclusion, jobs, steps and logs; never infer success from missing/partial/unrelated runs.
- Disk-storage changes require Linux and Windows CI. Preserve formatting verification, race-enabled tests, vet, build, coverage and artifact upload. Do not weaken checks.
- After completed CI, compare Linux/Windows aggregate statement coverage and per-function coverage from the same run. Explain discrepancies; inspect profiles for statement-level attribution if needed. Do not add artificial tests just to equalize percentages.
- CI formatting must be non-mutating and report offending files.
- Verify formatting, `go test -race -coverprofile=coverage.out ./...`, `go vet ./...`, and `go build ./...` through exact-SHA CI before claiming verified. No local Go execution is claimed unless actually run.
- Do not trigger manually controlled release CI unless explicitly asked.
- After CI analysis, update this file in a separate docs-only commit, including the checkpoint and next task. Markdown-only changes are not CI-tested.
- Keep verified facts separate from assumptions and open decisions. Do not claim unverified work is tested or pushed.

## Handoff hygiene
- This file is the durable source of truth, not a chat transcript. Maintain one concise current handoff in place.
- Once a handoff is resolved, remove it. Move lasting workflow rules here and substantial technical design into canonical `docs/` files; do not preserve history in this file.
- Handoff must record repo/branch, exact HEAD, code SHA and CI run, verification/coverage comparison, changes, risks/open decisions, next task and acceptance criteria, and whether `docs/storage_disk.md` matches implementation.
- If interrupted, state exact stopping point and uncommitted/unpushed state. Never present unverified work as done.

## Current handoff (2026-11-22)

- Repository: gunesis-game/pulsyflux-stream, branch main. Recent documentation checkpoints include `7848d507cd7e767c02d9e40f111bd1de65c174c7` (integration contract), `102fa9eceffa8745013f365d63b664441394a7cc` (async queue foundation docs), and `91c26da0a6d69b7a13d98a2aa8e0868b90bc42aa` (test checkpoint). Refresh main and inspect relevant files/tests/docs and exact HEAD before code edits.
- **Current code SHA:** `a16add6c53b7caa8ab30d331e0a5459d55a44a8a` (async writer foundation). Exact-SHA CI run [45000988988](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/45000988988) passed Linux and Windows formatting, race-enabled tests, vet, and build. Coverage artifacts from the same run: Linux 83.1%, Windows 83.1%; per-function coverage matched, with a small platform-dependent hit-count difference in the producer queue-wait statement. New async writer coverage: constructor 75%, Submit 91.7%, Close 100%, run 47.1%, fail 100%. This is foundational code only; no live persistence integration yet.
- Approved private storage root: `<current-working-directory>/pulsyflux-stream-data`, resolved to an absolute path when internal storage initializes. Library-created and privately managed; never expose path or configuration publicly.
- Approved asynchronous persistence: Publish submits a redacted record to a bounded private queue before live delivery. The private writer batches by internal size/time thresholds. Queue-full applies backpressure by waiting for capacity. Publish completion means queue admission and live delivery, not disk persistence/durability. A crash may lose queued or unflushed records. Preserve queue admission and disk append order. Individual Publish calls do not write to disk.
- **Approved writer ownership:** one process-wide private writer shared by all active EventChannels. An individual channel close does not stop it; final active channel close drains, flushes, and closes it. A later channel may initialize a new writer.
- **Approved storage failure behavior:** unrecoverable writer failure is terminal; retain the first cause, stop accepting persisted events, return errors wrapping that cause from subsequent persisted Publish calls, and publish a library-created `StreamWriteErrorEvent` to affected EventChannels' internal error streams. Error events are never persisted.
- **Approved error-stream architecture:** exactly one internal `ErrorStream` per `EventChannel`, shared by all error event streams for that channel. Consumers use the normal generic constructor/subscription API, e.g. `New[ErrorStream, StreamWriteErrorEvent](errorChannel, bufferSize)`. No separate ErrorChannel implementation, no special registration API, and no global error-channel registry. The library owns the event type and internal publishing path. Error delivery has reserved capacity independent of normal subscriber buffers.
- **Implemented foundation:** `storage_async.go` contains private bounded condition-variable queue, serialized worker, batch-size/idle flush, payload copy-on-admission, drain/close, and deduplicated failure-owner notification. `storage_async_test.go` tests queue ordering/drain, idempotent Close, payload ownership, and owner deduplication. The async writer is not connected to `Stream.Publish`, `EventChannel` construction/Close, shared writer owner-count lifecycle, or internal ErrorStreams. Remaining integration tests include saturation/backpressure, concurrent producers, idle timer flush, and injected disk failures.
- `docs/storage_integration_contract.md` now checkpoints foundation tests and lists integration tests still pending. `docs/storage_disk.md` was updated in commit `102fa9eceffa8745013f365d63b664441394a7cc` to document the async writer foundation and clearly state that live integration is pending.
- Code commits for this foundation: `a7970b6d45f98160ebdd251d19e3597eacd4901e` (initial async writer), `335c0e8b359a45627ddaabdebb14141893babcd8` (syntax fix), `8847ae7452c55a0a2fa24d2b7450e2952fea2e3a` (format pass checkpoint), `20a3f0451ff27e28b6fcd6f2817599955db119f9` (initial tests), `4c5116cf10bf4af29a5d6fe0a28e1d1631226c27` (payload/failure tests), and `a16add6c53b7caa8ab30d331e0a5459d55a44a8a` (queue cleanup fix). Final code SHA CI passed. No release/rollout authorized.
- Next task: integrate the async writer foundation. First add private writer-manager/root initialization and active-owner lifecycle; then connect typed Publish with preflight, redaction, queue admission and live delivery; implement channel-local ErrorStreams and failure routing. Ensure Close ordering and shared-writer final-owner semantics. Add focused tests for backpressure, concurrency, error notification and per/final-channel close. Replay remains out of scope. Verify exact-SHA Linux/Windows CI and compare coverage.
- Project remains pre-production and unreleased. No compatibility/migration obligation; do not bump module, release, or disk-format versions without an explicit reason.
- Working state: async writer foundation is committed and CI-verified; docs checkpoint is committed. Live persistence integration is still unimplemented.

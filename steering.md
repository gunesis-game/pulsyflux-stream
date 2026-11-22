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

- Repository: `gunesis-game/pulsyflux-stream`, branch `main`. Refresh HEAD and inspect the relevant source/tests/docs before another edit.
- **Latest code SHA:** `9f3c9d571149cc1b110436bfed90b599fe7bad45`. Private shared writer manager remains in `storage_manager.go`. `NewEventChannel[TStream]()` now acquires private process writer and returns `(*EventChannel[TStream], error)`. Channel stores the private writer reference and idempotent release closure. `EventChannel.Close()` now returns an error, closes owned streams then releases writer ownership; final-owner release drains/flushes/closes the writer. Repeated Close is idempotent. Test constructor helper handles error and cleanup.
- **Exact-SHA CI:** [run 45002835070](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/45002835070) passed Linux and Windows formatting, race-enabled tests/coverage, vet, and build.
- **Coverage comparison:** Linux and Windows total statement coverage were both **82.8%**. The coverage profiles were identical (502 entries, zero hit-count differences). Shared manager functions: `privateStorageRoot` 75.0%, `acquire` 81.0%, `releaseOnce` 93.3%, `acquireProcessWriter` 75.0%.
- **Code commits:** manager source `ed7261d676ce0aef4f8bc0a144f1e044d9e05f02`; manager tests `17b3b7c55e5a9d144d362c203aaa98229dae1d73`; constructor ownership commit `f472ae4111dfe4446c190a82a851b58d7fe7498a`; test constructor adaptation `99d447d82775002be4909038fb8e8ac243148d70`; gofmt checkpoint `5dd89c35bf5d3fd64e7bb15efdf9e364b1ecb5fe`; cleanup fix `9f3c9d571149cc1b110436bfed90b599fe7bad45`.
- **Documentation checkpoint:** `docs/storage_disk.md` ownership checkpoint `c5562d12f3ea82511c6146af57118f6e37ac90f7`; integration contract `28c40b39e67f19ddf773a01aedbb848bee49a4c4`. These docs-only commits are not CI-tested.
- **Still not live-integrated:** public `Stream.Publish` does not yet prepare redacted records or enqueue persistence. ErrorStream creation/failure routing remains pending. The channel `Get()` API and map-based registry still conflict with the steering architecture and should be removed in the coherent stream/channel redesign. Channel construction now creates/opens private storage, but it currently only acquires ownership; it does not persist Publish calls.
- **Next:** integrate persistence into Publish in order: validate and preflight subscribers, prepare the redacted JSON payload using Event-owned redaction, queue-admit an event record with channel/stream type identities and event creation timestamp, then deliver the live unredacted Event. Queue-full applies bounded backpressure; keep lock ordering race-safe. Add tests for persistence/redaction and no-subscriber persistence, queue saturation, concurrent Publish, and writer failure. Then implement channel-owned ErrorStreams and remove the map registry/Get API per steering, including channel Close behavior without a stream registry.
- Acceptance: exact-SHA Linux/Windows formatting, `go test -race -coverprofile=coverage.out ./...`, `go vet ./...`, and `go build ./...` pass; compare same-run coverage profiles. Update handoff after each verified checkpoint.
- Pre-production/unreleased. No release, rollout, module version, or disk-format version bump without explicit reason/authorization.
- Working state: async writer, private manager, and EventChannel ownership wiring committed and verified; Publish persistence integration and ErrorStreams remain pending.

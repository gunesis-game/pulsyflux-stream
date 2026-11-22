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
- **Normal stream subscription API:** consumers subscribe to system errors through the ordinary `Subscription`/`Subscribe` interface. There is no separate ErrorChannel implementation or special subscription mechanism. Since the error channel is library-owned and must not be exposed, `EventChannel[TStream]` provides `Errors(bufferSize)` to lazily create/return its channel-owned `*Stream[ErrorStream, StreamWriteErrorEvent]`; callers use `Subscribe(ctx)` on that returned stream. This is the explicit access boundary and supersedes the earlier contradictory requirement that consumers construct the error stream with `New[ErrorStream, StreamWriteErrorEvent](errorChannel, bufferSize)`.
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

- Repo: `gunesis-game/pulsyflux-stream`, branch `main`.
- Latest main HEAD: `70f6737b31e218636ed72abb6d4991aacc7b0e9e` (`docs: document owned ErrorStream access boundary`). This follows `5ab21d8832174191a59141380baf28c7635642bb`, which clarifies the owned error-stream access boundary.
- Latest verified code commit remains `ef0af7645e366591e5f3b50d7371055500459c9a`. Its exact-SHA CI run `45004383109` passed on Linux and Windows: formatting, race tests/coverage, vet, build, and artifact upload.
- Coverage comparison from that exact CI: Linux and Windows both **82.4% aggregate statement coverage**; per-file/function coverage reports match. Raw profiles differ only in hit counts.
- Documentation-only updates after the verified code commit: `02cc2f3f04df3669ce94184156917e8271a39e59` (README), `2614f61b12695336991f1536b6f1dde4fcecbd48` (storage integration contract), `5ab21d8832174191a59141380baf28c7635642bb`, and `70f6737b31e218636ed72abb6d4991aacc7b0e9e` (ErrorStream API boundary). CI is configured to ignore Markdown-only pushes, so these docs commits are not CI-tested.
- Implemented code: private async bounded writer, process-wide shared writer with active-owner count, channel writer ownership, redacted persisted Publish, zero-subscriber persistence, slow-subscriber preflight, and concurrent-producer persistence test.
- Remaining code milestone: implement ErrorStream terminal failure routing with reserved system-error capacity, then remove `Get()` and the stream registry in a coherent lifecycle change. Current `storageWriteFailed` only retains the first terminal cause. Removing the registry must preserve prompt subscription closure on EventChannel.Close; avoid replacing it with another channel/stream lookup registry.
- Next task: implement ErrorStream as a channel-owned internal system stream exposed only through `Errors(bufferSize)`; route terminal writer failure to affected EventChannel owners once, bypass persistence, preserve cause for `errors.Is/As`, and test terminal injected failures. Then remove `Get()` and map registry, using channel close signaling to shut subscriptions down without a registry. Run exact-SHA CI and compare coverage.
- No release/version bump; replay remains deferred. Review `docs/storage_disk.md` against implementation during the code milestone.

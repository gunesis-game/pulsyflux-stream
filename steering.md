# Steering: pulsyflux-stream

## Purpose
Maintain a small, typed, concurrency-safe Go event-stream library. Keep the live-stream core lightweight. Event payload validation/redaction belongs to Event; disk persistence is an internal subsystem.

## Agreed architecture and identity
- **Identity is type-derived, not UUID-derived.** `EventChannel[TStream]` identity is `EventChannel[TStream.name]` (e.g. `EventChannel[AuthStream]`). `Stream[TStream,TEvent]` identity is `Stream[TStream.name,TEvent]` (e.g. `Stream[AuthStream,AuthTokenValidEvent]`). Use a stable canonical type-name representation appropriate to Go types; do not rely on ambiguous bare names if package-qualified identity is needed to avoid collisions.
- `EventChannel` and `Stream` do not own or generate UUIDs. Do not add UUID recovery, identity catalogs, or deterministic UUID derivation.
- Event UUIDs remain for individual event identity unless the user explicitly changes that requirement.
- **ABSOLUTE PUBLIC-API RULE — DISK STORAGE MUST NEVER BE PUBLIC.** Disk storage is an internal implementation detail, not a consumer-facing dependency. Never expose a disk store type, constructor, interface, parameter, field, option, configuration object, or storage handle in the public API. Consumers must never construct, receive, configure, or pass a disk store. Do not suggest or implement `NewDiskStore(...)` or `NewEventChannel[TStream](store)`. This rule is explicit and overrides any earlier wording that could imply otherwise.
- **Store-backed construction:** the library creates/opens and owns its private disk storage internally. Channel construction loads the matching persisted channel record when it exists, or creates that record when absent. Loading the record is the lookup; there is no separate `Get()` operation for the same purpose. The channel must not maintain an in-memory registry/cache of channels or streams as a substitute for disk lookup.
- The public channel constructor must not accept storage arguments. Its public signature and error behavior must be designed around internally managed storage; do not preserve the old parameterless constructor requirement or `channel.NewStream()` lifecycle if they conflict with the corrected architecture.
- Remove the `Get()` API and map-based stream registry if they exist only to retrieve already-loaded streams. Do not add in-memory caching. Stream/channel lifecycle must follow persisted records and private store operations, not an auxiliary map.
- Disk-store construction, location, initialization, handles, and lifecycle remain entirely private. Constructor error propagation must be explicit without exposing storage types or handles.
- No schema fingerprint framework. Delete `schema.go` and its fingerprint composition/base/effective machinery. Do not replace it with another schema/catalog subsystem.
- Keep public generic relationship: `EventChannel[TStream]`, `Stream[TStream,TEvent]`, `Event[TEvent]`. One channel family can contain multiple streams with different event types.
- The event/stream/channel human-readable identity strings are type-derived and are used to identify persisted channel/stream records. They are not instance IDs.

## Ownership
- **Event (`event.go`):** typed payload, creation timestamp, redaction policy, payload assignment/validation, serialization/deserialization and event lifecycle. Move any Event-specific functionality currently in `schema.go` into `event.go`; do not leave a schema file behind.
- **Stream (`stream.go`):** concurrency-safe typed delivery of `Event[TEvent]` values to subscribers. Publish sends live unredacted events; persistence redacts before writing.
- **Channel (`channel.go`):** typed channel identity and store-backed construction/loading. Disk records are authoritative; no in-memory stream registry/cache and no channel UUID. A loaded channel record already represents lookup, so no parallel `Get()` is needed.
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
- Stream creation/loading is store-backed under the corrected channel lifecycle; do not implement an in-memory registry or duplicate-registration map as a lookup mechanism. The exact persisted stream-record operation and public constructor shape must match the disk-store design.
- Each stream delivers only `Event[TEvent]` to subscribers. Different `TEvent` streams coexist under one `TStream` channel.
- Publish is atomic across subscribers: if any subscriber is full, return `ErrSlowSubscriber` and deliver to none. Publish with zero subscribers succeeds.
- Preserve concurrency safety for Subscribe, Publish, cancellation, unsubscribe and Close.

## Disk storage principles
- Keep storage taxonomy explicit: lifecycle, write, read/scan, lookup, replay, maintenance, metadata.
- Prefer append-only segmented files. Make framing, checksum, segment rollover, sync policy and recovery boundaries explicit and versioned.
- Flush (application buffering) and Sync (OS/filesystem durability) are distinct; document durability windows.
- Storage handles redacted payload bytes and I/O; it does not own Go payload types or redaction.
- Begin with sequential scan and rebuildable/minimal indexes. No SQL/distributed infrastructure absent concrete requirements.
- Avoid parallel public event/stream/channel/record models unless a real boundary needs them.

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

- Repository: `gunesis-game/pulsyflux-stream`, branch `main`. Latest HEAD before this steering update: `44b112ef918e7aa943af312468b96473b69ac8b3`.
- Latest code SHA: `c9e0d705e4f43a3cc1dec599d785f33591984207`. CI run [44967119308](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44967119308) passed Linux and Windows: format, race tests/coverage, vet, build. Aggregate statement coverage was 86.6% on each OS. Both exact-SHA coverage profiles and per-function reports were extracted and compared: all 416 profile rows and the full reports are identical between Linux and Windows; no OS coverage discrepancy.
- Private `appendEvent[TStream,TEvent]` validates event/policy, requires payload set, serializes/redacts, derives type identities, and persists event creation time. Tests cover metadata, redaction round-trip, and failure cases. Not wired into `Publish` or channel lifecycle.
- Live `Publish` currently validates and preflights subscribers under stream mutex, then delivers the original event. EventChannel.Close marks closed and releases its mutex before closing streams. No writer lifecycle owner or replay dispatch exists.
- Storage v3 documentation: `docs/storage_disk.md`, `docs/type_identity.md`. Section 14 explicitly defers live persistence integration. README now links the new `docs/storage_integration_contract.md`.
- Integration contract draft recommends persist-before-delivery, synchronous backpressure/no queue, storage failure blocking delivery, per-stream sink, and startup-only replay separate from Publish. Replay may be deferred from first integration. It is design-only, no API/production wiring.
- Architecture correction from user (2026-11-22): disk is authoritative; channel construction loads an existing channel record or creates it if absent, so no separate `Get()` or in-memory channel/stream map/cache. **Disk storage MUST NEVER be made public.** The library internally creates/opens and owns disk storage; consumers never construct, receive, configure, or inject it. No public store type, constructor, interface, argument, field, option, or handle. Any earlier suggestion to pass a disk store into `NewEventChannel` was incorrect and is explicitly rejected. The earlier parameterless `NewEventChannel()` + `channel.NewStream()` proposal is also superseded. No implementation changes for this corrected lifecycle have started. Exact-SHA coverage comparison is complete: Linux and Windows reports/profiles are identical at 86.6% aggregate.
- Acceptance: no public API without authorization; CI green on every code SHA; disk architecture docs match implementation; private integration contract clearly labeled draft; checkpoint updated after work.
- Working state: committed changes, no uncommitted files known.

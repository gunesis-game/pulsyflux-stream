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
- Disk-store construction, location, initialization, handles, and lifecycle remain entirely private. Constructor error propagation must be explicit without exposing storage types or handles.
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

## Disk storage principles
- Keep storage taxonomy explicit: lifecycle, write, read/scan, lookup, replay, maintenance, metadata.
- Prefer append-only segmented files. Make framing, checksum, segment rollover, sync policy and recovery boundaries explicit. Use an internal format revision only when needed to interpret incompatible record encodings; it is not a release version and does not imply production stability.
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

- Repository: `gunesis-game/pulsyflux-stream`, branch `main`. Latest HEAD must be refreshed from GitHub before the next code change; do not rely on stale SHA values in this handoff.
- Latest code SHA: `c9e0d705e4f43a3cc1dec599d785f33591984207`. CI run [44967119308](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44967119308) passed Linux and Windows: format, race tests/coverage, vet, build. Aggregate statement coverage was 86.6% on each OS. Both exact-SHA coverage profiles and per-function reports were extracted and compared: all 416 profile rows and the full reports are identical between Linux and Windows; no OS coverage discrepancy.
- Private `appendEvent[TStream,TEvent]` validates event/policy, requires payload set, serializes/redacts, derives type identities, and persists event creation time. Tests cover metadata, redaction round-trip, and failure cases. Not wired into `Publish` or channel lifecycle.
- Live `Publish` currently validates and preflights subscribers under stream mutex, then delivers the original event. EventChannel.Close marks closed and releases its mutex before closing streams. No writer lifecycle owner or replay dispatch exists.
- Disk-format design documentation: `docs/storage_disk.md`, `docs/type_identity.md`. Internal record revision 3 is explicitly documented as an unreleased development encoding detail, not a product/release version. README carries a pre-production notice. Section 14 explicitly defers live persistence integration. README now links the new `docs/storage_integration_contract.md`.
- Integration contract draft recommends persist-before-delivery, synchronous backpressure/no queue, storage failure blocking delivery, per-stream sink, and startup-only replay separate from Publish. Replay may be deferred from first integration. It is design-only, no API/production wiring.
- Architecture clarification from user (2026-11-22): **event records are the ONLY persisted record type.** There are no channel records or stream records. Channel and stream identities exist only as metadata on event records. An empty channel/stream has no persisted entity. Do not add channel/stream record formats, catalogs, manifests, or synthetic records. The library privately owns disk storage; consumers never construct, receive, configure, or inject it. No public store type, constructor, interface, argument, field, option, or handle. Do not add a channel/stream lookup cache or registry as a substitute for persisted entities. The integration contract and implementation must respect this event-only model. The private storage-root policy remains open; it does not justify inventing channel records. Exact-SHA coverage comparison is complete: Linux and Windows reports/profiles are identical at 86.6% aggregate.
- Acceptance: no public API without authorization; CI green on every code SHA; disk architecture docs match implementation; private integration contract clearly labeled draft; checkpoint updated after work.
- Project status: pre-production, unreleased, and not live. README and disk-format docs now state this explicitly. No compatibility or migration obligations. No module/release/format version bumps for ordinary development changes; only change an internal format revision for a real incompatible record-encoding change.
- Documentation commits: `950fb239` (steering project status/version policy), `4915f177` (README pre-production notice), `47069965` (disk-format revision clarified as development-only). These are docs-only; no CI run is expected from markdown-only path filters.
- Working state: committed changes, no uncommitted files known.

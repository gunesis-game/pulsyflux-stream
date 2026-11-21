# Steering: pulsyflux-stream

## Purpose and scope
Maintain a small, typed, concurrency-safe Go event stream. Event redaction is implemented for the private serialization boundary. Durable storage, indexing, schema cataloging, and replay are internal capabilities to be implemented incrementally while preserving the lightweight live-stream core.

## Ownership boundaries
- **Event:** owns typed payload, event-creation time, redaction policy, payload assignment/validation, and event lifecycle. It does not own JSON serialization or deserialization.
- **Stream:** owns concurrent delivery of complete in-memory events. Never redact during Publish.
- **Channel / registry:** owns logical grouping and in-memory typed stream registration. Channel UUID identity is internal infrastructure, not a public persistence contract.
- **Schema:** `schema.go` is the intermediary between typed runtime objects and the system of record. It owns the generic schema contract, canonical descriptors, dependency composition, base/effective fingerprint derivation, and the generic mapping/resolution contract that turns EventChannel/Stream schema identities into persisted instance identities. It also owns redaction policy/validation/mechanics and generic JSON encode/decode. Event, Stream, and EventChannel owning files provide only type-specific inputs/hooks; do not add per-type schema files or duplicate serialization/fingerprint logic.
- **Storage:** the system of record implements the persistence side of the schema mapping contract: durable channel/stream identity metadata and event records, segmented disk files, durability, reading/scanning, recovery, and rebuildable indexes/catalog metadata. It receives schema identities and private instance IDs from the schema/runtime boundary; it must not independently derive Go type schemas or decide schema equivalence.
- **Replay:** reads historical records and dispatches through existing stream/channel abstractions. Replay must not be re-recorded by the same recorder by default.

## Identity and schema invariants
- Keep event, stream, and channel instance UUIDs internal. Consumers must not inspect, generate, or persist these IDs.
- Instance identity is distinct from schema identity. Runtime UUIDs identify the recovered or newly created channel/stream instances; schema fingerprints identify the logical schema used to resolve them. A persisted record carries both. Do not substitute a schema fingerprint for the private instance UUID.
- `TStream` is itself the channel-family schema. Its deterministic base schema fingerprint resolves the persisted channel mapping used by `NewEventChannel[TStream]`. Stream identity is then resolved only within that parent channel; the durable stream identity is the pair (resolved channel identity, resolved stream identity). A stream schema fingerprint alone is not a globally unique stream key. The consumer does not supply an ID or decide whether the channel or stream is new or previously persisted.
- There must be a durable mapping from each `EventChannel[TStream]` runtime object to its system-of-record channel identity/metadata, mediated by `schema.go`: `TStream` base fingerprint resolves the persisted channel mapping, and the recovered/created private channel UUID identifies that channel instance.
- There must also be a durable mapping from each `Stream[TStream, TEvent]` runtime object to its system-of-record stream identity/metadata, mediated by `schema.go`: within the resolved channel, the stream schema identity resolves the persisted stream mapping, and the recovered/created private stream UUID identifies that stream instance. The same stream schema in two different channels represents two distinct persisted stream instances.
- These mappings are foundational, not optional enhancements to event append/replay. A disk event record's six schema fingerprints and UUID fields do not by themselves implement the channel/stream mapping or allow constructors to recover those runtime objects. Do not claim transparent recovery is implemented until both mappings are persisted, resolved, validated, and tested end to end.
- Fingerprint-based resolution is internal package/storage behaviour. Do not expose public Open/Find/Search operations or require consumers to inspect storage records. The package transparently recovers the existing private channel UUID and metadata when the `TStream` fingerprint is already present; otherwise it creates a new private channel identity and record.
- Keep the `TStream` lookup fingerprint distinct from the aggregate Channel schema fingerprint. The former identifies the channel-family schema for initial resolution; the latter composes the channel context and registered stream schemas and is persisted/validated as record schema identity. Stream-level schema identity similarly resolves the appropriate persisted stream identity.
- SHA-256 fingerprints are deterministic and collision-resistant, not a mathematical guarantee of uniqueness. When resolving a fingerprint, validate the canonical descriptor/identity in the persisted metadata rather than assuming a hash match alone proves equivalence.
- Structural schema fingerprints are private deterministic SHA-256 hashes of canonical descriptors, layered as data → event → stream → channel with an explicit fingerprint-format version. Never hash raw memory, rely on unstable reflection strings, or use random UUIDs as schema fingerprints.
- Descriptors capture exported serialized fields, field types, and JSON tags; omit unexported and json:"-" fields; normalize struct field order by JSON field name. Struct names/package paths are omitted for structural equivalence; named scalar types retain name/package identity. Descriptor/fingerprint changes are versioned changes.
- Fingerprints detect structural definition changes; they do not provide migrations or prove semantic compatibility. Current descriptors do not capture custom MarshalJSON/UnmarshalJSON behavior or external codec semantics.
- Use full cryptographic hashes. Do not truncate or force hashes into UUID formatting without a separately justified format.
- One channel-wide persisted sequence defines historical order across all streams in that channel; stream replay filters this global order. Sequence is authoritative. The record timestamp is event-creation time (not disk-write time) and is metadata for filtering/diagnostics, never an ordering substitute.
- Redaction must happen before persistence. Persisted redacted data cannot restore excluded secrets.

## Storage taxonomy and direction
- Keep storage taxonomy explicit: Lifecycle (Open/Close), Write (Append/Flush/Sync), Read (Scan/Read), Lookup (Find/Seek), Replay (Replay/Resume), Maintenance (Recover/Rotate/Compact/Prune/Verify), Metadata (catalog/index).
- Prefer append-only segmented files. Make record framing, checksums, segment rollover, sync policy, and recovery boundaries explicit and versioned.
- Distinguish application-buffer Flush from OS/filesystem Sync durability. Document the durability window for each policy.
- Storage backends handle already-redacted JSON payload bytes and disk I/O; they must not own Go payload types, schema encoding/decoding, or redaction rules. Storage records own persistence metadata (IDs, sequence, event-creation timestamp, six schema fingerprints, framing/checksum). Do not duplicate schema type identity or timestamp inside the payload.
- Start with sequential scan and rebuildable/minimal indexes. Do not add SQL, distributed infrastructure, or a public storage API without a concrete requirement and an agreed contract.
- Avoid parallel public event, stream, channel, or storage-record models unless a concrete boundary requires one.

## Development and release compatibility
- `main` is the development line. Make the public API and source changes required by the agreed design, including intentional breaking changes to existing consumer-facing interfaces.
- Do not preserve, emulate, or constrain a required new API merely to maintain backward compatibility on `main`, unless the user explicitly requests a compatibility requirement.
- Existing consumers are protected by the release workflow, not by freezing the development API. The configured release CI is manually triggered by the user as the controlled validation step before release; do not trigger or assume a release run unless explicitly instructed.
- Keep ordinary development CI and release CI distinct. A passing development CI run does not imply release validation or publication. Never claim existing consumers are affected by un-released changes on `main`.
- When a deliberate breaking API change is made, update affected source, tests, examples, and documentation together, and clearly record the API change and its verification state in the current handoff.

## Public API constraints
Expected typed event/channel/stream relationship:
- `EventChannel[TStream]` is the typed logical parent for a family of related streams.
- One `EventChannel[TStream]` can relate multiple streams: `Stream[TStream, TEvent1]`, `Stream[TStream, TEvent2]`, `Stream[TStream, TEvent3]`, and so on.
- Every stream registered with a given channel uses that channel's same `TStream`. `TStream` is the channel/stream-family type context and contributes to stream and channel schema identity; it is not the event payload type.
- Each stream supplies its own `TEvent`, which determines its typed payload and event schema identity. Event instances remain `Event[TEvent]`.
- Keep stream operations strongly typed for their respective `TEvent`; do not collapse the public API to `Stream[TStream]` or make `EventChannel` non-generic.
- The channel may relate streams with different event types; do not constrain all streams in one channel to share a single `TEvent`.
- `NewEventChannel[TStream]` is the single consumer-facing channel creation entry point. Through the schema.go-to-system-of-record mapping contract, it must transparently resolve an existing channel by the `TStream` base schema fingerprint or create and persist a new channel mapping; callers must not choose between create/open, supply UUIDs, or perform find/search operations.
- `New[TStream, TEvent](channel, bufferSize)` remains the stream registration entry point. Internally it resolves an existing stream using its schema identity and restores its private stream UUID/metadata, or creates a new stream identity when no persisted match exists. The consumer does not need to know whether the stream was previously persisted.
- Preserve private channel and stream instance identities. The generic type relationship and schema fingerprints are not substitutes for the runtime UUIDs. Storage lookup/index mechanics and constructor error propagation must be designed internally; do not expose storage APIs to solve them.

Expected event API:
- NewRedactionPolicy(exclude ...string) RedactionPolicy
- NewEvent[T](redactionPolicy RedactionPolicy) (Event[T], error)
- (*Event[T]).SetData(T) error
- (*Event[T]).Data() T
Keep type-name derivation, timestamp, serialization/deserialization, validation, instance IDs, and schema fingerprints private. Do not introduce an event envelope: the payload is the event's redacted data directly; schema identity is validated separately, and sequence/timestamp live only in the storage record. Do not add public Validate/Serialize/Deserialize/TypeName methods or restore public event struct fields for payload/time/type metadata.
Do not introduce a public storage API until the internal storage contract and durability semantics are designed and tested.

## Redaction invariants
- Every Event is constructed with a RedactionPolicy; an empty policy is explicit opt-out.
- Policy entries are exclusion paths expressed as exact JSON field names/tags. Copy the input slice and keep the policy immutable.
- Validate paths in NewEvent[T] and again in SetData before assigning payload. Reject blank, duplicate, malformed, and unresolved paths.
- Apply redaction only to a serialized JSON copy; never mutate live event data.
- Deserialization cannot recover excluded values.
- Current traversal support: structs, pointers, slices/arrays of structs. Maps and embedded-field flattening remain out of scope until deliberately designed and tested.

## Stream behavior constraints
- Each `Stream[TStream, TEvent]` delivers only `Event[TEvent]` values to its subscribers.
- A channel can register multiple streams with the same `TStream` and different `TEvent` parameters.
- Publish sends the full, unredacted event to each active subscriber.
- A full subscriber buffer rejects Publish atomically with ErrSlowSubscriber; no subscriber receives that rejected event.
- Publishing with zero subscribers succeeds.
- Preserve concurrency safety for Subscribe, Publish, cancellation, unsubscribe, and Close.
- Channel identity is logical identity, not durable storage by itself. Its `TStream` base fingerprint provides the internal recovery key; its private UUID identifies the particular persisted channel instance. Channel creation/recovery must remain transparent to consumers.

## Mandatory implementation and CI rules
- Work directly on `main`. Do not create branches unless the user explicitly asks.
- Before editing, verify the current `main` HEAD, recent commits, relevant source/tests, and latest CI state. Do not rely on stale handoffs.
- Implement one coherent change at a time. Commit and push directly to `main`.
- After every code-changing push, identify the CI run for that exact commit SHA. Wait for completion and inspect the conclusion, all jobs, all meaningful steps, and logs. Never infer success from a missing, queued, partial, or unrelated run.
- CI must run on **both Linux and Windows** for supported disk-storage changes. Preserve formatting verification, race-enabled tests, vet, build, coverage profiles/reports, and artifact upload as configured. Do not weaken checks to get a green run.
- **Mandatory cross-platform coverage comparison after every completed CI run:** compare Linux and Windows aggregate statement coverage and per-function coverage from that same run. Identify shared functions whose coverage differs, review platform-specific functions/branches separately, and explain whether differences are expected or indicate missing tests. Inspect profile artifacts for statement-level attribution when per-function output is insufficient. Record the comparison and conclusion in the current handoff. Do not add artificial tests merely to make percentages identical; tests must cover real behavior and failure paths.
- If CI fails, diagnose the root cause, fix it, push a correction, and verify the new exact SHA. Retain a failed attempt only when its cause remains a useful lesson or explains the current state; once resolved, remove its obsolete handoff/action items.
- After CI analysis, update this file in a separate documentation-only commit with the current verified checkpoint and next actionable task. Do this whether CI passed or failed. Clearly label pending/failed verification. Markdown-only changes do not trigger CI; never claim they were CI-tested.
- Before reporting completion, verify formatting, `go test -race -cover ./...`, `go vet ./...`, and `go build ./...` through the exact CI run. If execution is unavailable, say verification is unconfirmed; do not claim local execution unless it actually occurred.
- CI formatting verification must be non-mutating and print offending filenames. Never run a formatter in CI before checking the committed tree.
- Do not invent gaps to fill a handoff. Separate verified facts, hypotheses, recommendations, and decisions requiring user input.

## Mandatory session-handoff hygiene

- `steering.md` is the durable source of truth for current work, not an append-only chat transcript.
- Keep **one current authoritative handoff** for the active task. Update it in place after each iteration; do not append a new section for every completed step.
- **When a hand-off/task is successfully resolved and verified, remove its completed hand-off section and obsolete next steps.** Do not preserve a completed handoff as history.
- Before removing a resolved handoff, classify the durable knowledge it contains:
  - Put lasting AI workflow instructions, architectural guardrails, and recurring implementation lessons into the appropriate directive-oriented section of `steering.md`.
  - Put substantial technical design, architecture, format, lifecycle, or operational knowledge in the appropriate document under `./docs`; update an existing canonical document where one exists rather than creating duplicates.
  - Keep only concise, generally applicable instructions in `steering.md`; do not turn it into a design-history archive or duplicate detailed documentation.
- A failed CI attempt may be summarized in the current handoff while its correction is pending. Once corrected and verified, remove the resolved failure narrative unless it teaches a lasting rule; encode that rule in the general instructions or relevant `./docs` document.
- The current handoff must state: repository/branch; exact current HEAD; exact code SHA and CI run tested; final CI result and Linux/Windows coverage comparison; changes made; post-CI analysis; remaining confirmed gaps/risks and unresolved decisions; next task with files/components, prerequisites, tests, and acceptance criteria; whether `docs/storage_disk.md` matches the implementation.
- If work is interrupted, record the exact stopping point, pending checks, uncommitted/unpushed state, and safest next action. Never describe uncommitted work as pushed or unverified work as tested.
- Keep this file directive-oriented and concise. Do not duplicate README walkthroughs or retain resolved session narratives.

## Current handoff — generic channel and schema identities implemented (2026-11-21)

### Repository and verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; work directly on `main`, no branches.
- `main` HEAD immediately before this handoff update: `88e860db630d2e81bede9127e49d1881f349c1ae` (documentation-only commit; this handoff update is also documentation-only).\n- Current code SHA: `4c432a2eefc858c0289a0392bea79684059be846`; CI run #217 remains the verified code checkpoint.
- CI run #217 ([44918459724](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44918459724)) tested that exact SHA and passed on Linux and Windows.
- Both platforms passed non-mutating formatting verification, `go test -race -coverprofile=coverage.out ./...`, per-function coverage reporting, artifact upload, `go vet ./...`, and `go build ./...`.
- Aggregate statement coverage: Linux 84.4%, Windows 84.4%. Both reports contained 60 function entries and every per-function percentage matched. No platform-specific coverage discrepancy was found.
- No local Go test execution is claimed; GitHub Actions is the verification source.
- `docs/storage_disk.md` remains aligned with `storage_disk.go`; no disk-record format changes were made.

### Implemented generic API and registry ownership
- `EventChannel[TStream]` is the typed parent for multiple `Stream[TStream, TEvent]` values. All streams owned by one channel share `TStream`; each stream has its own `TEvent`. Events remain `Event[TEvent]`.
- `Stream` and `Subscription` carry both type parameters. Subscribe, Publish, and event delivery remain strongly typed.
- Generic package-level constructors/lookups are `New[TStream, TEvent](channel, bufferSize)` and `Get[TStream, TEvent](channel)`.
- The channel owns and synchronizes its private heterogeneous registry, keyed by reflected `TEvent` type. Heterogeneous storage is private; the public API returns typed streams.
- Duplicate registration of the same `TEvent` within one channel returns `ErrDuplicateStream`; different event types coexist.
- `EventChannel.Close` marks the channel closed, detaches its streams under the channel lock, then closes them outside the lock.
- The package-global `defaultStore` and standalone `store.go` have been removed. Do not repurpose the old in-memory event store as a stream registry; stream registration belongs to the channel. Disk persistence is separate in `storage_disk.go`.
- Channel UUID remains private and separate from schema fingerprints.
- README and tests use the generic API. Tests register three different event types under one channel and preserve isolation, duplicate, close, delivery, cancellation, and concurrency behavior.

### Implemented schema identity composition
- Event base/effective identities are derived from `TEvent` structure and that Event's normalized redaction paths.
- Stream base/effective identities include the canonical `TStream` context and the Event base/effective identity for the event being recorded.
- Channel base identity includes the `TStream` context and all registered stream base identities, sorted deterministically.
- For a record, Channel effective identity includes the active stream's event-specific effective identity; other registered streams contribute their default effective identity (empty redaction policy). This makes event-level policy variation explicit without freezing one policy on a stream or channel.
- `Stream.schemaFingerprintsFor(event)` derives all six record fingerprints (Event, Stream, Channel base/effective) from runtime objects. The disk writer already accepts the six-value `schemaFingerprints` input, but the recorder/writer lifecycle is not yet connected.
- Added `docs/schema_identity.md` describing the contract and its per-record policy behavior.\n- Added `docs/record_scope.md` defining the system-of-record lookup model: resolve the channel from the `TStream` base schema identity, then resolve a stream within that channel, yielding the stream's event-record collection. The two-stage model is logical; storage may use a composite index without changing scope semantics.
- Removed the obsolete layer-only `schemaFingerprint` helper; runtime identity composition now uses the shared base/effective identity contract.

### Remaining confirmed gaps and risks
- **Foundational mapping is not implemented:** `schema.go` currently derives canonical descriptors/fingerprints and performs redaction/JSON encoding/decoding, but it has no schema-to-system-of-record resolver or durable mapping contract for `EventChannel[TStream]` or `Stream[TStream, TEvent]`.
- `channel.go` currently generates a fresh random channel UUID in `NewEventChannel` and does not resolve/persist a channel mapping by the `TStream` base fingerprint.
- `New`/`register` currently creates an in-memory stream and does not resolve/persist a stream mapping by stream schema identity.
- `storage_disk.go` currently persists event records containing UUIDs and six schema fingerprints, but those event records do not constitute the missing channel/stream identity mapping/catalog. There is no durable constructor recovery path.
- The six fingerprints are derivable, but no recorder lifecycle currently connects Publish/Event to `diskWriter.Append`.
- Private storage location/ownership and writer lifecycle per logical channel remain unresolved. Do not invent a path, global directory, or public configuration API.
- `SyncEachAppend` remains unsupported pending a reliable, documented, tested directory-entry durability contract on both Linux and Windows.
- Event policy variation is defined per record. Other streams in the same channel contribute their empty-policy effective identity when computing that record's Channel effective fingerprint.

### Agreed direction: transparent schema-fingerprint recovery
- The consumer uses one public channel entry point: `NewEventChannel[TStream]()`. There is no separate public create/open choice and no public channel Find/Search operation.
- Internally, derive the deterministic base schema fingerprint for `TStream`. Use it to resolve the logical channel's persisted record. If found, recover its private channel UUID and metadata; if absent, create a new private UUID and initialise the channel record.
- The `TStream` base fingerprint is the channel-family lookup identity. Stream resolution is scoped by the resolved parent channel; never globally resolve a stream from its schema fingerprint alone. Do not confuse the channel-family lookup fingerprint with the aggregate Channel base/effective fingerprints, which compose the channel context and registered stream schemas for record identity.
- `New[TStream, TEvent](channel, bufferSize)` similarly resolves or creates the stream's private identity using the stream schema identity within the resolved channel. Resolve-or-create must be atomic; concurrent constructors for one mapping converge on the same persisted identity. Mapping failures and canonical descriptor conflicts must be returned, never converted into a fresh UUID.
- Persisted fingerprints and canonical descriptors are internal recovery metadata. Fingerprints are deterministic and collision-resistant; verify canonical identity on a match rather than claiming mathematical collision impossibility.
- `schema.go` mediates schema-to-instance mapping and validates recovered schema descriptors; storage implements durable lookup/catalog operations, metadata writes, and recovery. Runtime channel/stream constructors coordinate these private contracts and restore/create UUIDs. Do not expose storage records, IDs, or lookup operations in the public API.
- `docs/record_scope.md` now specifies channel-scoped stream identity, atomic resolve-or-create semantics, descriptor validation, constructor error propagation, and failure consistency. The current `NewEventChannel[TStream]()` pointer-only signature cannot report durable mapping failures; implementation must intentionally change it to return `(*EventChannel[TStream], error)`. The exact storage namespace/index mechanics, writer ownership/lifecycle, and filesystem location remain to be designed. Do not silently invent a public key or filesystem path.

### Next task
1. Review and confirm the mapping contract in `docs/record_scope.md`, especially the intentional `NewEventChannel` error-return signature change and atomic cross-instance resolve-or-create requirement.
2. Design private system-of-record catalog ownership and the schema.go/storage mapping interface: catalog location, atomic create-if-absent primitive, metadata format/versioning, corruption handling, and lifecycle/close ownership. Do not invent a filesystem path or public configuration API.
3. Implement channel mapping, then channel-scoped stream mapping, restoring private UUIDs and preserving typed in-memory duplicate semantics.
4. Connect `Stream.schemaFingerprintsFor(event)` and the redacted payload to `diskWriter.Append` without moving schema/redaction responsibilities into storage.
5. Add end-to-end tests for automatic channel/stream recovery, concurrent same-schema resolution (including independent runtime instances sharing one system of record), append/replay with all six fingerprints, per-event redaction changes, multiple streams in one channel, channel close, and writer failures.
6. Keep `SyncEachAppend` unsupported until cross-platform directory-entry durability is explicitly designed and tested.
7. Verify each code-changing SHA through Linux/Windows CI and compare aggregate plus per-function coverage. Do not trigger the manually controlled release workflow.

Acceptance: both runtime-object-to-system-of-record mappings are durable and mediated by `schema.go`; consumers use the same public constructors whether records exist or not; `TStream` base fingerprint transparently resolves the logical channel; stream schema identity transparently resolves stream identity within that channel; private UUIDs are restored rather than exposed; aggregate six-fingerprint event-record identity and redaction boundaries remain intact; and exact-SHA Linux/Windows CI passes with coverage comparison.

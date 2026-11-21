# Steering: pulsyflux-stream

## Purpose and scope
Maintain a small, typed, concurrency-safe Go event stream. Event redaction is implemented for the private serialization boundary. Durable storage, indexing, schema cataloging, and replay are planned capabilities; implement them incrementally and preserve the lightweight live-stream core.

## Ownership boundaries
- **Event:** owns typed payload, derived event type identity, internal timestamp, required redaction policy, payload assignment/validation, and private serialization/deserialization.
- **Stream:** owns concurrent delivery of complete in-memory events. Never redact during Publish.
- **Channel / registry:** owns logical grouping and in-memory typed stream registration. Channel UUID identity is internal infrastructure identity, not a public persistence contract.
- **Schema fingerprinting:** privately derives deterministic structural fingerprints for payload/event, stream, and channel schema layers. Fingerprints identify definitions, not runtime instances. Do not expose fingerprint values or internal UUIDs in the consumer API.
- **Storage:** planned internal subsystem. It should append serialized records to disk, manage segment lifecycle and durability, read/scan and decode records, recover incomplete tails, and maintain rebuildable indexes/catalog metadata.
- **Replay:** planned capability that reads historical records and dispatches through existing stream/channel abstractions. Replay must not be re-recorded by the same recorder by default.

## Identity and schema invariants
- Keep event, stream, and channel instance UUIDs internal to this package and infrastructure. Consumers must not need to inspect, generate, or persist these IDs.
- Instance identity is distinct from schema identity. Runtime instances must not share history merely because they share a schema fingerprint.
- Structural schema fingerprints are implemented privately in schema.go. They are deterministic SHA-256 hashes of canonical descriptors, layered as data → event → stream → channel with an explicit fingerprint-format version. Never hash raw memory, rely on unstable reflection strings, or use random UUIDs as schema fingerprints.
- Structural descriptors capture exported serialized fields, field types, and JSON tags, omit unexported and json:"-" fields, and normalize struct field order by JSON field name. Struct names/package paths are omitted for structural equivalence; named scalar types retain name/package identity. Treat descriptor/fingerprint format changes as versioned changes. Tests cover determinism, structure/tag changes, field-order normalization, layered identity, and recursive types.
- Fingerprints detect definition changes; they do not provide migrations or prove semantic compatibility. Current descriptors do not capture custom MarshalJSON/UnmarshalJSON behavior or external codec semantics; treat fingerprints as structural hints until codec identity/version is incorporated.
- Use full cryptographic hashes for schema fingerprints. Do not truncate or force hashes into UUID formatting unless a separately justified format is designed.
- Persisted records need an explicit record-format version, schema fingerprint, event identity, internal stream/channel identity, ordering information, timestamp, and encoded payload as the storage design is implemented.
- Sequence numbers, not wall-clock timestamps or UUIDs, are the authoritative persisted order within a stream. Timestamps remain useful for filtering and diagnostics.
- Redaction must be applied before persistence. Persisted redacted data cannot restore excluded secrets.

## Storage taxonomy and implementation direction
- Keep storage taxonomy explicit: Lifecycle (Open/Close), Write (Append/Flush/Sync), Read (Scan/Read), Lookup (Find/Seek), Replay (Replay/Resume), Maintenance (Recover/Rotate/Compact/Prune/Verify), Metadata (catalog/index).
- Prefer append-only segmented files for the initial disk backend. Make record framing, checksums, segment rollover, sync policy, and recovery boundaries explicit and versioned.
- Schema fingerprints are groundwork only; do not treat them as a persistence implementation. Start storage with sequential scan and rebuildable/minimal indexes; avoid introducing SQL tables or distributed infrastructure prematurely.
- Distinguish application-buffer Flush from OS/filesystem Sync durability. Document the durability window for each policy.
- Storage backends handle encoded records and disk I/O; they must not own Go payload types or redaction rules.
- Avoid adding parallel public event, stream, channel, or storage-record models unless a concrete boundary requires one.

## Public API constraints
Expected event API:
- NewRedactionPolicy(exclude ...string) RedactionPolicy
- NewEvent[T](redactionPolicy RedactionPolicy) (Event[T], error)
- (*Event[T]).SetData(T) error
- (*Event[T]).Data() T
Keep type-name derivation, timestamp, envelope, serialize, deserialize, validation, instance IDs, and schema fingerprints private. Do not add public Validate/Serialize/Deserialize/TypeName methods or restore public event struct fields for payload/time/type metadata.
Do not introduce a public storage API until the internal storage contract and durability semantics are designed and tested.

## Redaction invariants
- Every Event is constructed with a RedactionPolicy; an empty policy is explicit opt-out.
- Policy entries are exclusion paths expressed as exact JSON field names/tags.
- Copy policy slices at construction; callers cannot mutate the stored list through their original slice. Keep policy immutable after construction.
- Validate paths in NewEvent[T], and validate again in SetData before assigning payload.
- Reject blank, duplicate, malformed, and unresolved paths.
- Apply redaction only to a serialized JSON copy; never mutate live event data.
- Deserializing serialized redacted data cannot recover excluded values. Do not imply otherwise.
- Current traversal support: structs, pointers, slices/arrays of structs. Maps and embedded-field flattening are out of scope until deliberately designed and tested.

## Stream behavior constraints
- Publish sends the full, unredacted event to each active subscriber.
- A full subscriber buffer rejects a Publish atomically with ErrSlowSubscriber; no subscriber receives that rejected event.
- Publishing with zero subscribers succeeds.
- Preserve concurrency safety for Subscribe, Publish, cancellation, unsubscribe, and Close.
- Channel identity is logical identity, not durable storage by itself.

## Implementation and documentation practices
- Prefer standard library JSON/reflection; keep dependencies minimal.
- Add focused tests for schema fingerprint determinism, structural changes, JSON tags, nested types, and recursion; also maintain redaction serialization and stream concurrency/lifecycle coverage.
- Keep README factual and consumer-focused. Keep this file directive-oriented; avoid duplicating the README walkthrough or API examples.
- Before reporting completion, run gofmt validation, go test -race -cover ./..., go vet ./..., and go build ./.... If execution is unavailable, report verification as unconfirmed.
- CI should print offending filenames when formatting fails; do not hide gofmt output.


## Session handoff — storage design and schema fingerprints (2026-11-21)

### Decisions agreed with the user
- Public API/type hierarchy remains `Channel[TStream]`, `Stream[TEvent]`, and `Event[TData]`. These are the existing abstractions; do not invent parallel public models.
- Instance UUIDs for event, stream, and channel are internal package/infrastructure details. Consumers should not depend on or persist these IDs themselves.
- Keep instance identity distinct from structural schema identity. Schema fingerprints are deterministic and represent definitions; instance UUIDs identify concrete runtime instances.
- Schema fingerprints should be derived from canonical type structure, not SQL table schemas. The SQL analogy was illustrative only.
- Storage design should focus on I/O/disk operation taxonomy and lifecycle: event capture, encode, append, flush/sync, read/scan/load, lookup, replay, and maintenance/recovery.
- Initial storage direction: append-only segmented files, explicit versioned framing/checksum, durability policy, sequence-based ordering, minimal/rebuildable indexes, and recovery-safe tail handling. Avoid SQL/distributed infrastructure until justified.
- Replay should deliver stored events via existing stream/channel abstractions and avoid being recorded again by the same recorder by default.

### Implementation progress
- On `main`, added private structural fingerprint groundwork in `schema.go` and focused tests in `schema_test.go`.
- Updated this steering file to direct future storage work and preserve public API encapsulation.
- Fingerprint tests are intended to cover determinism, structural/tag changes, field-order normalization, layer distinction, and recursive types.
- Fingerprints are currently structural hints: custom JSON marshal/unmarshal behavior and external codec identity are not represented. They do not imply compatibility or migration support.
- Previous session reported CI checks passing (gofmt, race-enabled tests/coverage, vet, build); in the next session, verify the actual latest main commit and CI state before relying on that report.

### Next session: continue implementation on main
1. Re-fetch repository state and `steering.md`; verify current `main`, recent commits, and CI.
2. Inspect existing `Event[TData]`, `Stream[TEvent]`, `Channel[TStream]`, redaction/serialization, and repository file structure. Avoid assuming the earlier proposed types or disk layout are final.
3. Design the physical on-disk record format before coding: canonical framing, format version, schema fingerprint, internal event/stream/channel identities, sequence, timestamp, codec/type metadata, payload bytes, and checksum. Decide which fields are actually required and document tradeoffs.
4. Define segment lifecycle and durability semantics: active segment, rollover trigger, buffering, Flush versus Sync, incomplete-tail recovery, and middle-of-file corruption policy.
5. Implement the smallest vertical slice: internal writer appending versioned records to an append-only segment, with tests for round-trip encoding, corruption detection, partial-tail recovery, and ordering. Keep the public API unchanged.
6. Run required checks locally if possible: gofmt validation, `go test -race -cover ./...`, `go vet ./...`, and `go build ./...`; then verify CI.

### Guardrails for the next session
- Start from actual repository code; preserve existing type definitions and behavior.
- No exported UUID/schema fingerprint accessors and no consumer-facing persistence IDs.
- Do not start with a database or full indexing engine; establish the record/segment contract first.
- Do not silently drop middle-of-file corruption. Incomplete trailing writes may be recoverable at a valid record boundary; report corruption.
- Treat this handoff as a continuation checkpoint, not permission to implement beyond the next agreed design step without validating assumptions.

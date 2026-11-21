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


## Session handoff — storage writer and reader checkpoint (2026-11-21)

### Current branch and checkpoint
- Continue directly on `main` in `gunesis-game/pulsyflux-stream`; do not create a branch unless the user asks.
- Latest verified GitHub commit from this session's inspection: `f401b3a69fd4e4fb9a17cce666925e11b3f9bcba` (`fix: correct malformed io test import`). Confirm HEAD and CI afresh; prior chat statements about run #127 passing may be unreliable.
- Earlier commits in this storage slice include `d1cb667` (multi-segment reader), `7fc4fb4` (reader tests), and subsequent test import corrections. Verify actual tree contents, not only commit messages.
- Prior attempts to add reader tests introduced malformed imports (escaped newline and `io""`) and multiple corrective commits. Inspect `storage_disk_test.go` carefully and ensure formatting/imports are actually valid.

### Implemented storage groundwork (verify from current files before relying on this)
- `storage_disk.go`: versioned framed record codec, CRC checksum, scan/recovery logic, append-only segment writer, rollover, flush/sync/close, and sequential multi-segment reader.
- `storage_disk_test.go`: codec/checksum, incomplete-tail handling, writer sequence recovery/rollover, empty-segment recovery, and multi-segment reader tests.
- Storage IDs and schema fingerprints remain private. Keep public event/stream/channel API unchanged.
- Writer sequence recovery was hardened to scan all segments and reject sequence regressions and incomplete tails in non-active segments.
- Reader scans segments sequentially, detects ordering violations and corruption, and returns `io.EOF` at end. Confirm current behavior against actual source; test limitations/edge cases remain.

### CI status caution
- Chat history claims CI run #127 passed, but this checkpoint has not independently confirmed that run's completed job details. Verify the run on GitHub before treating it as green.
- CI workflow formats Go sources and then checks formatting, race-enabled tests/coverage, vet, and build. Avoid modifying CI just to hide failures. Diagnose logs, fix source/tests, commit to main, and confirm the new run.

### Immediate next-session steps
1. Fetch `steering.md`, branch HEAD, recent commits, workflow runs, and the complete current source/tests. Verify the live repository state.
2. Inspect `storage_disk.go` and `storage_disk_test.go`; run local commands if a working checkout is available: `gofmt -w storage_disk.go storage_disk_test.go`, `gofmt -l .`, `go test -race -cover ./...`, `go vet ./...`, and `go build ./...`. If no local environment is available, use CI logs and be explicit about what cannot be run.
3. Review reader correctness: empty segments; filenames and segment numbering gaps/duplicates; sequence monotonicity across segment boundaries; incomplete tail handling; complete-frame corruption; close semantics; and whether loading an entire segment into memory is acceptable.
4. Fix reader/writer edge cases with focused tests, commit directly to `main`, and wait for CI. Do not claim green until the run's actual status and job conclusions are confirmed.
5. Only after reader reliability is established, design a rebuildable minimal index (likely sequence-to-segment/offset), with no database or public storage API. Discuss the contract before broad implementation.

### Guardrails
- Preserve the agreed public API and identity boundaries: consumers must not inspect or generate event/stream/channel UUIDs or schema fingerprints.
- No parallel public models, no public storage API, and no persistence IDs exposed to consumers.
- Sequence numbers are authoritative order; timestamps are metadata.
- Redact before persistence; never imply excluded values can be recovered.
- Incomplete trailing writes may be recovered only under explicit frame-boundary rules; middle corruption must fail, not be skipped.
- Keep this handoff actionable and factual; confirm claims from the live repo in the next session.

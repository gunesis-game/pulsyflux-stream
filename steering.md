# Steering: pulsyflux-stream

## Purpose and scope
Maintain a small, typed, concurrency-safe Go event stream. Event redaction is implemented for the private serialization boundary. Durable storage, indexing, schema cataloging, and replay are internal capabilities to be implemented incrementally while preserving the lightweight live-stream core.

## Ownership boundaries
- **Event:** owns typed payload, event-creation time, redaction policy, payload assignment/validation, and event lifecycle. It does not own JSON serialization or deserialization.
- **Stream:** owns concurrent delivery of complete in-memory events. Never redact during Publish.
- **Channel / registry:** owns logical grouping and in-memory typed stream registration. Channel UUID identity is internal infrastructure, not a public persistence contract.
- **Schema:** `schema.go` owns the generic schema contract, descriptors, dependency composition, base/effective fingerprint derivation, redaction policy/validation/mechanics, and generic JSON encode/decode. Event, Stream, and EventChannel owning files provide only type-specific inputs/hooks; do not add per-type schema files or duplicate JSON serialization.
- **Storage:** internal subsystem for serialized records, segmented disk files, durability, reading/scanning, recovery, and rebuildable indexes/catalog metadata.
- **Replay:** reads historical records and dispatches through existing stream/channel abstractions. Replay must not be re-recorded by the same recorder by default.

## Identity and schema invariants
- Keep event, stream, and channel instance UUIDs internal. Consumers must not inspect, generate, or persist these IDs.
- Instance identity is distinct from schema identity. Runtime instances must not share history merely because they share a schema fingerprint.
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
- Publish sends the full, unredacted event to each active subscriber.
- A full subscriber buffer rejects Publish atomically with ErrSlowSubscriber; no subscriber receives that rejected event.
- Publishing with zero subscribers succeeds.
- Preserve concurrency safety for Subscribe, Publish, cancellation, unsubscribe, and Close.
- Channel identity is logical identity, not durable storage by itself.

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

## Current handoff — schema consolidation and envelope removal (2026-11-21)

### Repository and verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; work directly on `main`, no branches.
- Current code HEAD: `99077e389232f02ab5721e2fe84496d0c558bf86` (`refactor: introduce shared base and effective schema identities`).
- CI run #192 (`44911479446`) tested that exact SHA and passed. Linux and Windows jobs both completed formatting verification, race-enabled tests/coverage, per-file coverage reporting, artifact upload, vet, and build. Cross-platform aggregate/per-function coverage still needs inspection from this run's artifacts before treating the comparison as complete.
- This handoff update is documentation-only; it is not covered by the code CI run.
- No local Go test execution is claimed.

### Agreed architecture and implementation rules
- `schema.go` is the single owner of the generic schema contract and generic JSON encoding/decoding. Use the generic operation names `encodeSchema` and `decodeSchema`; do not introduce `encodeEventSchema`/`decodeEventSchema`.
- Do not create `event_schema.go`, `stream_schema.go`, or `channel_schema.go`. Remove the existing `event_schema.go` after consolidating its generic mechanisms into `schema.go`. Event/Stream/EventChannel source files may supply type-specific runtime state or schema inputs/hooks, but never JSON serialization.
- The schema contract covers `Event[TData]`, `Stream[TEvent]`, and `EventChannel[TStream]`. The current non-generic EventChannel must become generic; this is an intentional breaking API change and must be reflected in callers, tests, and docs.
- Redaction is integral to the shared schema model. Shared policy representation, validation, redaction mechanics, and fingerprint derivation belong in `schema.go`; type-specific code supplies only what the generic contract cannot infer.
- Each Event, Stream, and EventChannel has base and effective fingerprints. Dependency composition propagates nested base/effective schema changes. All six values remain independently available to private storage validation; do not collapse them into one.
- Fingerprints identify definitions, not runtime instances. Keep fingerprints and internal UUIDs private. Fingerprints do not prove redaction was applied; persist only redacted payloads.
- The event envelope must be removed completely. Persist the event's redacted data directly as JSON; do not wrap it in duplicate type/data/time metadata. Schema type identity is derived/validated by the schema contract, not duplicated in payload metadata.
- Storage record owns EventID, StreamID, ChannelID, channel-wide Sequence, event-creation Timestamp, all six fingerprints, and record framing/checksum. Keep timestamp in the record as the event's creation time (not append/write time); remove timestamp from the payload. Sequence is authoritative persisted order; timestamp is diagnostic/filter metadata.
- Successful Stream.Publish includes recording in the accepted publish path. One channel-wide sequence establishes historical order across streams. Stream replay filters that history; channel replay scans it once and dispatches matching records to existing stream subscribers in sequence order. Replay is not re-recorded by the same recorder by default.
- Preserve live Publish atomicity, fail-closed recording/replay, and existing subscriber delivery semantics. Replay preserves dispatch order, not cross-stream application processing order across independent goroutines.
- Private storage location/ownership and writer lifecycle per logical channel are still unresolved. Do not invent a path, global directory, or public configuration API. Resolve this before connecting recorder lifecycle to Publish.
- `SyncEachAppend` remains unsupported on Linux and Windows until directory-entry durability can be documented and tested consistently.

### Confirmed implementation state and next work
- Current `schema.go` has shared redaction helpers and base/effective identity composition, but generic encode/decode is not yet consolidated there.
- Current `event_schema.go` still contains `eventEnvelope`, type-specific encode/decode, and event schema logic; remove it after migration.
- Current `diskRecord` still has one `SchemaFingerprint` and duplicate `Type`; change the private record format deliberately to the six fingerprints and remove `Type`. Update `docs/storage_disk.md` in the same code change.
- Current `diskWriter.Append` accepts schema/type strings and timestamp separately; revise it to accept storage-owned metadata and already encoded/redacted payload according to the consolidated schema contract.
- Before recorder integration, settle storage directory ownership/lifecycle. Do not silently proceed with an invented persistent path.
- Next implementation sequence: (1) inspect all schema/event/storage tests and call sites; (2) move generic JSON encode/decode and event schema inputs into `schema.go`, remove the envelope and `event_schema.go`; (3) update disk record/version/tests/docs for six fingerprints and no Type field; (4) implement generic EventChannel API and dependency identity propagation; (5) resolve private storage ownership before recorder integration; (6) then implement recording/replay.
- Add tests for direct redacted payload JSON, no envelope metadata, event creation timestamp retained only in diskRecord, base/effective identity propagation at all three layers, six-field record validation/round-trip, generic channel compile/API use, and failure paths.
- Verify the exact code SHA with Linux and Windows CI. Inspect coverage artifacts and compare aggregate plus per-function coverage; do not force percentages to match. Update this handoff in place after CI analysis. Do not trigger the manually controlled release workflow.
- Acceptance: `schema.go` alone owns generic encode/decode; no per-type schema files remain; no event envelope or duplicate payload Type/timestamp; disk records retain event timestamp and all six fingerprints; intentional public API changes are reflected in tests/docs; development CI verified on the exact SHA; storage ownership decision is documented before recorder wiring.

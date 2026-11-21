# Steering: pulsyflux-stream

## Purpose and scope
Maintain a small, typed, concurrency-safe Go event stream. Event redaction is implemented for the private serialization boundary. Durable storage, indexing, schema cataloging, and replay are internal capabilities to be implemented incrementally while preserving the lightweight live-stream core.

## Ownership boundaries
- **Event:** owns typed payload, internal timestamp, redaction policy, payload assignment/validation, and event lifecycle. Schema-aware serialization/deserialization and event type identity are centralized in `schema.go`.
- **Stream:** owns concurrent delivery of complete in-memory events. Never redact during Publish.
- **Channel / registry:** owns logical grouping and in-memory typed stream registration. Channel UUID identity is internal infrastructure, not a public persistence contract.
- **Schema fingerprinting:** privately derives deterministic structural fingerprints for payload/event, stream, and channel schema layers. Fingerprints identify definitions, not runtime instances. Never expose fingerprint values or internal UUIDs in the consumer API.
- **Storage:** internal subsystem for serialized records, segmented disk files, durability, reading/scanning, recovery, and rebuildable indexes/catalog metadata.
- **Replay:** reads historical records and dispatches through existing stream/channel abstractions. Replay must not be re-recorded by the same recorder by default.

## Identity and schema invariants
- Keep event, stream, and channel instance UUIDs internal. Consumers must not inspect, generate, or persist these IDs.
- Instance identity is distinct from schema identity. Runtime instances must not share history merely because they share a schema fingerprint.
- Structural schema fingerprints are private deterministic SHA-256 hashes of canonical descriptors, layered as data → event → stream → channel with an explicit fingerprint-format version. Never hash raw memory, rely on unstable reflection strings, or use random UUIDs as schema fingerprints.
- Descriptors capture exported serialized fields, field types, and JSON tags; omit unexported and json:"-" fields; normalize struct field order by JSON field name. Struct names/package paths are omitted for structural equivalence; named scalar types retain name/package identity. Descriptor/fingerprint changes are versioned changes.
- Fingerprints detect structural definition changes; they do not provide migrations or prove semantic compatibility. Current descriptors do not capture custom MarshalJSON/UnmarshalJSON behavior or external codec semantics.
- Use full cryptographic hashes. Do not truncate or force hashes into UUID formatting without a separately justified format.
- Sequence numbers, not timestamps or UUIDs, define persisted order within a stream. Timestamps are metadata for filtering and diagnostics.
- Redaction must happen before persistence. Persisted redacted data cannot restore excluded secrets.

## Storage taxonomy and direction
- Keep storage taxonomy explicit: Lifecycle (Open/Close), Write (Append/Flush/Sync), Read (Scan/Read), Lookup (Find/Seek), Replay (Replay/Resume), Maintenance (Recover/Rotate/Compact/Prune/Verify), Metadata (catalog/index).
- Prefer append-only segmented files. Make record framing, checksums, segment rollover, sync policy, and recovery boundaries explicit and versioned.
- Distinguish application-buffer Flush from OS/filesystem Sync durability. Document the durability window for each policy.
- Storage backends handle encoded records and disk I/O; they must not own Go payload types or redaction rules.
- Start with sequential scan and rebuildable/minimal indexes. Do not add SQL, distributed infrastructure, or a public storage API without a concrete requirement and an agreed contract.
- Avoid parallel public event, stream, channel, or storage-record models unless a concrete boundary requires one.

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
- **When a hand-off/task is successfully resolved and verified, remove its completed hand-off section and obsolete next steps.** Preserve only durable architecture decisions, constraints, and lessons that still affect future work. Do not retain stale checkpoints, superseded instructions, old CI status cautions, or completed work lists.
- A failed CI attempt may be summarized in the current handoff while its correction is pending. Once corrected and verified, remove the resolved failure narrative unless it teaches a lasting rule; encode that rule in the general instructions instead.
- The current handoff must state: repository/branch; exact current HEAD; exact code SHA and CI run tested; final CI result and Linux/Windows coverage comparison; changes made; post-CI analysis; remaining confirmed gaps/risks and unresolved decisions; next task with files/components, prerequisites, tests, and acceptance criteria; whether `docs/storage_disk.md` matches the implementation.
- If work is interrupted, record the exact stopping point, pending checks, uncommitted/unpushed state, and safest next action. Never describe uncommitted work as pushed or unverified work as tested.
- Keep this file directive-oriented and concise. Do not duplicate README walkthroughs or retain resolved session narratives.

## Storage architecture documentation
- `docs/storage_disk.md` is the canonical architecture guide for disk storage, including its Mermaid diagrams.
- Whenever storage code changes behavior, data structures, framing/encoding, segment naming/lifecycle, recovery, durability/failure handling, reader behavior, or invariants, update `docs/storage_disk.md` in the **same code commit**.
- Update relevant diagrams, component/source maps, and invariants; do not merely append a note. Cross-check every claim against final source and tests.
- Label deferred behavior and limitations clearly. Never document proposed behavior as implemented.
- For unrelated changes, do not edit the architecture guide. Markdown-only updates do not trigger CI; paired Go + Markdown storage changes must trigger normal CI.

## Current storage durability status

- `SyncEachAppend` is deliberately rejected on **both Linux and Windows** with `errDirectorySyncUnsupported`, before the storage directory or segment files are created. The former platform-specific directory-sync helpers were removed.
- Ordinary buffered append, flush, explicit active-file `Sync()`, segment rotation, and recovery remain available on Linux and Windows. Explicit file sync does not guarantee persistence of the segment's directory entry; never describe it as strict per-append crash durability.
- Do not re-enable `SyncEachAppend` until both platforms have reliable, documented, tested directory-entry durability implementations and the same contract is verified by CI.
- Windows research found no documented, supported standard-user Win32/native API sequence for flushing directory namespace metadata with the required crash-durability guarantee. Do not introduce an undocumented directory-handle flush or infer durability from an API call merely returning success. Revisit strict durability through a separately designed architecture or a clearly scoped, user-approved operational requirement.
- The current cross-platform behavior is intentionally uniform and fail-closed; strict per-append durability remains unavailable rather than being silently weakened.

## Current handoff — schema-aware event codec centralized; replay next (2026-11-21)

### Repository and verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; work directly on `main`, no branches.
- Exact code SHA tested: `44c7ee35221899017b4edaaaceae06c65706518f`.
- CI run [44903635287](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44903635287), run #178, completed successfully for that exact SHA. Cleanup and both Linux/Windows jobs passed formatting, race-enabled tests, coverage report generation, artifact upload, vet, and build.
- Aggregate coverage: Linux **83.9%**, Windows **83.9%**. All 54 per-function entries match. Statement profiles have the same 464 blocks; one `stream.go` publish-send block has execution counts 807 vs. 806, but is covered on both platforms. This is an expected count variation, not a missing-coverage difference.
- No local Go test execution is claimed. This codec change does not alter disk framing or persisted record layout; `docs/storage_disk.md` remains applicable and unchanged.

### Completed codec ownership change
- `Event[T]` remains the typed domain model in `event.go`: payload, timestamp, redaction policy, payload assignment, validation, and access.
- `schema.go` owns the private event envelope, event type-name derivation, schema-aware redaction/serialization, schema fingerprint derivation for the event layer, and decoding with persisted fingerprint/type validation.
- `encodeEventSchema` returns the opaque encoded envelope plus its schema fingerprint and type name for internal persistence. `decodeEventSchema` requires the fingerprint stored with the record and rejects mismatches before reconstructing the typed event.
- Redaction policy is intentionally supplied by the registered typed stream during decode; it is not persisted in the envelope. Persisted redacted data cannot restore excluded values.
- Tests exercise the schema codec directly, preserve redaction and timestamp round-trip behavior, and reject a mismatched fingerprint.
- Storage remains opaque and does not own payload types or redaction rules. No public serializer, deserializer, fingerprint, UUID, or storage-record API was added.

### Next task: design channel/stream replay delivery
- Replay is a package-level channel/stream capability, not direct storage access for consumers. Do not expose storage records, segment paths, internal event IDs, fingerprints, or event-level lookup APIs.
- Replay must publish reconstructed events to existing subscribers through their normal subscription channels, as agreed. Do not introduce a separate replay-only consumer channel.
- Before implementation, inspect current storage lifecycle/recorder integration and design an ordering boundary that prevents replay/live interleaving gaps or duplicates. Prefer a persisted high-water sequence boundary: replay records through the boundary, then deliver buffered live events after it, preserving sequence order.
- Define channel replay (all persisted events in the channel) and typed stream replay (events for the selected registered stream). Respect channel/stream instance identity, schema fingerprint, event type, current redaction policy, segment rotation, and restart sequence continuity.
- Keep the append-only log authoritative. Start with sequential scan/filtering; do not add an index unless measured replay performance establishes a need.
- Before coding, specify lifecycle/concurrency behavior for Subscribe, Publish, Close, slow subscribers, recorder errors, and replay errors. Add tests for replay/live races, no duplicates or gaps, channel-vs-stream filtering, ordering across rotations/restarts, schema mismatch, redaction, and Windows/Linux parity.
- Preserve the existing live Publish atomicity guarantee and all public event/stream/channel APIs except an explicitly agreed replay entry point. Do not re-record replayed events by the same recorder by default.

### Durable constraints
- `SyncEachAppend` remains rejected on both Linux and Windows until documented, reliable directory-entry durability exists and is tested identically on both platforms.
- Do not implement Find/Seek, SQL, distributed infrastructure, segment pruning, or public storage APIs without a concrete requirement and agreed contract.

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
- **When a hand-off/task is successfully resolved and verified, remove its completed hand-off section and obsolete next steps.** Do not preserve a completed handoff as history.
- Before removing a resolved handoff, classify the durable knowledge it contains:
  - Put lasting AI workflow instructions, architectural guardrails, and recurring implementation lessons into the appropriate directive-oriented section of `steering.md`.
  - Put substantial technical design, architecture, format, lifecycle, or operational knowledge in the appropriate document under `./docs`; update an existing canonical document where one exists rather than creating duplicates.
  - Keep only concise, generally applicable instructions in `steering.md`; do not turn it into a design-history archive or duplicate detailed documentation.
- A failed CI attempt may be summarized in the current handoff while its correction is pending. Once corrected and verified, remove the resolved failure narrative unless it teaches a lasting rule; encode that rule in the general instructions or relevant `./docs` document.
- The current handoff must state: repository/branch; exact current HEAD; exact code SHA and CI run tested; final CI result and Linux/Windows coverage comparison; changes made; post-CI analysis; remaining confirmed gaps/risks and unresolved decisions; next task with files/components, prerequisites, tests, and acceptance criteria; whether `docs/storage_disk.md` matches the implementation.
- If work is interrupted, record the exact stopping point, pending checks, uncommitted/unpushed state, and safest next action. Never describe uncommitted work as pushed or unverified work as tested.
- Keep this file directive-oriented and concise. Do not duplicate README walkthroughs or retain resolved session narratives.

## Current handoff — generic schema extensions, redaction fingerprints, and channel-wide replay design (2026-11-21)

### Repository and verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; work directly on `main`, no branches.
- Current HEAD before this handoff edit: `da9e144f828a8495a83162d9bd1925cc511bd31e`.
- Last code SHA: `ed1911b87813bbe79ea31fedf698a68e268c62bf`; CI run [44908089974](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44908089974), run #191, passed Linux and Windows.
- That run reported 83.9% aggregate statement coverage on both platforms; coverage profiles and per-function coverage were identical. The handoff edit is documentation-only and does not trigger CI.
- No implementation of the newly agreed generic schema/fingerprint/replay architecture has been made. No local Go test execution is claimed.
- `docs/storage_disk.md` still describes the current single-schema-fingerprint disk record and current private writer/reader; it does not yet describe the proposed multi-fingerprint record format. It remains accurate for the current implementation, not the agreed future design.

### Agreed architecture — requirements, not yet implemented

#### Shared schema contract and type extensions
- `schema.go` owns one generic schema contract and reusable mechanisms. `event_schema.go`, `stream_schema.go`, and `channel_schema.go` extend that contract with only type-specific schema information and behaviour; they must not duplicate facilities already supplied centrally.
- The schema model covers `Event[TData]`, `Stream[TEvent]`, and `EventChannel[TStream]`. The current source has a non-generic `EventChannel`; reconcile this explicitly as part of the planned generic channel change rather than silently treating current code as already compliant.
- Redaction is part of this shared schema model. The central layer owns common policy representation, validation, redaction mechanics, and fingerprint derivation; type-specific extensions declare how policy applies to their respective schemas.
- Each of the three schema layers has both a **base schema fingerprint** (before redaction) and an **effective schema fingerprint** (after the applicable redaction policy). Redaction can change the effective schema fingerprint. Dependency composition must ensure changes to a nested schema or its effective redaction are reflected by dependent Stream and EventChannel fingerprints.
- The six identities (base/effective for Event, Stream, and EventChannel) must be available to the private disk record/storage validation path. Do not collapse them into one fingerprint or assume a combined fingerprint makes the individual identities unnecessary.
- Fingerprints identify schema definitions, not runtime instances. Preserve private instance identity boundaries and do not expose fingerprints or internal UUIDs through consumer APIs.
- Persisted payloads must be redacted before disk append. Fingerprints identify the schema/redaction view; they do not independently prove that redaction was correctly applied.

#### Recording, ordering, and replay
- A successful `Stream.Publish()` includes recording as part of the successful publish path. Accepted live events receive their persisted sequence at the same ordering boundary.
- `EventChannel` is a logical scope, not a publish/subscribe endpoint. Publish and subscribe remain at individual Stream level.
- Channel-level replay is orchestration across streams registered to the channel. It initiates delivery through each stream's existing subscribers; it is not a separate channel subscription, channel delivery buffer, or alternate publish endpoint.
- One channel-wide sequence defines historical order across all streams. Stream replay filters that history while preserving the selected stream's relative order. Channel replay scans the global sequence and dispatches each record to its matching stream in that order. Never replay streams independently and merge their results.
- The sequence is assigned at the channel-level ordering point. Replay dispatch preserves cross-stream order; independently consumed subscriber channels/goroutines do not guarantee application processing order across streams.
- Replay must not be re-recorded by the same recorder by default. Preserve existing live Publish atomicity and fail-closed storage/replay behaviour.

### Confirmed gaps and unresolved decisions
- Current `schema.go` has structural layer fingerprints but no base/effective distinction or redaction-aware effective fingerprint contract.
- Current `event_schema.go` computes only an Event fingerprint and owns event-envelope encoding/decoding. No `stream_schema.go` or `channel_schema.go` exists.
- Current `diskRecord` in `storage_disk.go` stores one `SchemaFingerprint`, not six layer/base-effective identities. Record format/version and validation need a deliberate change.
- Current `EventChannel` in `channel.go` is non-generic. The agreed `EventChannel[TStream]` model requires a source/API design change.
- The disk writer assigns sequence under its own writer mutex and is not integrated with Stream Publish or a channel-wide ordering coordinator.
- Persistent storage location/ownership and writer lifecycle per logical channel remain unresolved. Do not invent a path, public configuration API, or implicit global storage directory. Resolve this design before wiring recording into Publish.
- Current disk writer buffers successful appends by default; do not claim crash durability. `SyncEachAppend` remains rejected on Linux and Windows until the documented cross-platform directory-entry durability requirement is met.
- No replay/recording implementation has been added. `docs/storage_disk.md` must be updated in the same code commit as any disk record/framing/reader/writer behaviour change.

### Next task and acceptance criteria
1. Design the generic schema contract in `schema.go`: schema descriptors/composition, base/effective fingerprint derivation, and shared redaction semantics. Keep type-specific extensions thin and non-duplicative.
2. Design `Event[TData]`, `Stream[TEvent]`, and generic `EventChannel[TStream]` schema extensions, including dependency composition and compatibility checks.
3. Before implementing recorder integration, resolve private persistent storage location/ownership and the channel-level ordering/lifecycle boundary.
4. Extend private disk record metadata to carry and validate all six fingerprints; version the record format deliberately and update `docs/storage_disk.md` in the same code commit.
5. Add focused tests for base fingerprint stability, effective fingerprint changes under redaction, dependency fingerprint propagation, schema mismatch at each layer, serialization/deserialization, and redaction before persistence.
6. For recording/replay, test channel-wide sequence assignment under concurrent publishes, channel replay ordering across streams and segment rotations/restarts, stream filtering, replay/live handoff without gaps or duplicates, replay non-recording, slow subscribers, close/cancellation, and fail-closed recorder/replay errors.
7. Run the configured Linux and Windows CI on the exact code SHA. Compare aggregate and per-function coverage and inspect profile artifacts for unexplained differences. Update this handoff after analysis; when the task is fully resolved, move lasting instructions/design knowledge into the correct steering sections or `./docs`, then remove the completed handoff section.


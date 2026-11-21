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

## Current handoff — restore shared schema codec (2026-11-21)

### Repository and verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; work directly on `main`, no branches.
- Current code HEAD: `3f56fc1a83ade904188079bc951d8d6ea5b25644` (`fix: restore generic schema codec after envelope removal`).
- This code commit restores `eventSchemaIdentity`, `encodeSchema`, and `decodeSchema` in `schema.go` after commit `720e73e` removed their definitions while leaving tests that call them. Redaction now uses normalized paths; decode checks both base and effective identities.
- CI verification for exact code SHA `3f56fc1a83ade904188079bc951d8d6ea5b25644` is **unconfirmed**. The available commit-status query returned no statuses, and the available commit-workflow-run query only reports pull-request-triggered runs; it did not expose the push-triggered CI run. Do not infer that CI passed or failed. No local Go test execution is claimed.
- Linux/Windows aggregate and per-function coverage comparison is pending because the exact-SHA CI run and its artifacts have not been surfaced.
- The following steering update is documentation-only and is not CI-tested.

### Changes made
- Restored generic schema encoding and decoding in `schema.go`; no per-type schema adapter file was reintroduced.
- `encodeSchema` serializes the payload directly, applies redaction to a JSON copy, and returns the shared event-layer identity.
- `decodeSchema` validates the expected base/effective identity before decoding already-redacted JSON.

### Post-push verification state
- Exact commit checked: `3f56fc1a83ade904188079bc951d8d6ea5b25644`.
- Available combined commit statuses: none returned.
- Available commit-associated workflow runs: none returned; that endpoint is limited to pull-request-triggered runs and therefore does not establish the state of the push-triggered CI workflow.
- Required checks still to inspect on the exact SHA: non-mutating formatting verification; Linux and Windows race-enabled tests and coverage; per-file coverage reports/artifacts; `go vet ./...`; and `go build ./...`.
- Once the run is found, inspect every job, meaningful step, and relevant log. Compare Linux and Windows aggregate statement coverage and per-function coverage from that same run; inspect profiles for statement-level attribution where needed. Record whether differences are expected or reveal missing coverage. Do not force percentages to match.

### Remaining confirmed gaps and risks
- `storage_disk.go` still persists only `SchemaFingerprint` and `Type`; the intended record contract requires all six independent base/effective fingerprints (data, event, stream, channel layers as defined by the agreed contract) and no duplicate `Type` field.
- `docs/storage_disk.md` still documents the old single-fingerprint plus Type record format and old Append arguments. It does **not** match the intended schema/storage contract.
- `EventChannel` remains non-generic and exposes its UUID as a public `ID`; `Stream.New` still accepts `*EventChannel`. This conflicts with the agreed generic channel API and internal-identity boundary.
- Higher-layer stream/channel schema descriptors and dependency composition are not yet wired through the current runtime types.
- Private storage location/ownership and writer lifecycle per logical channel remain unresolved. Do not invent a path, global directory, or public configuration API. Resolve this before connecting recorder lifecycle to Publish.
- `SyncEachAppend` remains unsupported on Linux and Windows until directory-entry durability can be documented and tested consistently.
- No local Go test execution is claimed.

### Next task
1. First locate and inspect the push-triggered CI run for exact SHA `3f56fc1a83ade904188079bc951d8d6ea5b25644`. If it failed, diagnose and correct it before starting another implementation change. If run details/artifacts remain inaccessible, keep verification explicitly unconfirmed.
2. After exact-SHA CI analysis, implement the private disk-record schema change in `storage_disk.go` and its tests: replace the single schema fingerprint with the six independently validated fingerprints; remove the duplicate `Type`; preserve EventID, StreamID, ChannelID, channel-wide Sequence, event-creation Timestamp, direct redacted JSON payload, framing, and checksum semantics.
3. Update `docs/storage_disk.md` in that same code change so record fields, writer arguments, examples, and schema boundary match the implementation. Add round-trip, missing-field, mismatch, and version/format tests. Treat the record format change as deliberate and version it explicitly where required; do not silently reinterpret existing version-1 records.
4. Verify the new exact code SHA with Linux and Windows CI, inspect all jobs/logs, and complete the mandatory cross-platform aggregate/per-function coverage comparison.
5. Keep storage ownership/lifecycle as a separate unresolved prerequisite before recorder integration. Then implement generic `EventChannel[TStream]` registration and propagate base/effective dependency identities through Event, Stream, and Channel layers, updating callers/tests/docs together.
6. Do not trigger the manually controlled release workflow.

Acceptance for the active storage/schema work: `schema.go` alone owns generic encode/decode and redaction mechanics; no per-type schema files; no event envelope or duplicate payload Type/timestamp; disk records retain event-creation timestamp and all six fingerprints; channel identity remains private; `docs/storage_disk.md` matches implementation; and the exact code SHA has verified Linux/Windows CI plus a documented coverage comparison.

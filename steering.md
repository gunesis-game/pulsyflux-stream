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

## Current handoff — heterogeneous channels and private identity (2026-11-21)

### Repository and verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; work directly on `main`, no branches.
- Current `main` HEAD and code SHA: `1a498c57dc19493659dabc25400bbddf02bbf83f` (`style: remove trailing blank line from channel test`).
- CI run #211 (`44914465680`) tested this exact SHA and **passed**. Linux and Windows both passed non-mutating formatting verification, `go test -race -coverprofile=coverage.out ./...`, per-function coverage reporting, artifact upload, `go vet ./...`, and `go build ./...`.
- Both platforms reported 84.7% aggregate statement coverage. Per-function coverage matched across Linux and Windows; no cross-platform discrepancy was found. Both coverage artifacts were uploaded.
- No local Go test execution is claimed; CI is the verification source.
- `docs/storage_disk.md` still matches the version-2 six-fingerprint record implementation.

### Changes made
- Confirmed the heterogeneous channel decision: one `EventChannel` can own many different typed streams. Keep `EventChannel` non-generic; `New[T]` and `Get[T]` remain generic functions. This avoids pretending one channel has one payload type and fits Go's lack of independently generic methods.
- Made the channel UUID private (`id`). Removed public `ID` and `NewEventChannelWithID`; channel identity can no longer be inspected, supplied, or reconstructed by consumers.
- The in-memory registry still keys streams by private channel UUID plus reflected event type. Duplicate registration of the same event type in one channel remains rejected; different event types remain independent.
- Updated tests and README to cover/document heterogeneous registration and private identity.

### Post-CI analysis
- Exact-SHA CI run #211 completed successfully on both platforms. Formatting, race tests, coverage, vet, and build all passed.
- Linux and Windows both reported 84.7% aggregate statement coverage. All per-function percentages matched. No profile-level comparison was needed because no per-function platform differences appeared.
- A prior push failed only the formatting gate due to one trailing blank line in `stream_test.go`; it was removed and the final exact-SHA run passed. This is resolved, not a pending action.

### Remaining confirmed gaps and unresolved design
- Runtime schema identities are still only fully implemented for Event. Stream and Channel base/effective identities are not yet propagated into runtime objects, so the six disk fingerprints are not yet sourced end-to-end from the live Event → Stream → Channel composition.
- Redaction policy is currently supplied per Event, not configured on a Stream. Before deriving Stream/Channel effective fingerprints, decide and document how varying event redaction policies within one typed stream affect the effective identities. Do not silently freeze the first policy or allow identity drift without a contract.
- Private storage location/ownership and writer lifecycle per logical channel remain unresolved. Do not invent a path, global directory, or public configuration API. Resolve before connecting recorder lifecycle to Publish.
- `SyncEachAppend` remains unsupported on Linux and Windows pending documented and tested directory-entry durability.

### Next task
1. Design the schema dependency contract for heterogeneous channels: Event base/effective identities, Stream base/effective identities derived from Event dependencies, and Channel base/effective identities derived deterministically from the set of registered typed streams. Explicitly define the effect of per-Event redaction policies on Stream/Channel effective identities.
2. Implement the agreed identity composition in `event.go`, `stream.go`, `channel.go`, `store.go`, and `schema.go`; keep hashing/composition centralized in `schema.go`. Keep UUIDs private and separate from schema fingerprints.
3. Add tests for two or more differently typed streams, deterministic channel identity independent of registration order, dependency changes, redaction-policy behavior, and schema mismatch. Ensure the six disk fingerprints can be populated from runtime identities without inventing storage ownership/lifecycle.
4. Update README and relevant docs with any intentional API/contract changes. Verify exact-SHA Linux/Windows CI and compare aggregate plus per-function coverage. Do not trigger the manually controlled release workflow.

Acceptance: heterogeneous typed registration remains intact; Event/Stream/Channel each have base/effective schema identities with deterministic dependency composition; policy changes follow the explicitly documented contract; channel instance UUID remains private; the six disk fingerprints are derivable from the runtime objects; and exact code SHA passes Linux/Windows CI with coverage comparison.

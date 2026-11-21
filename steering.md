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
Expected typed event/channel/stream relationship:
- `EventChannel[TStream]` is the typed logical parent for a family of related streams.
- One `EventChannel[TStream]` can relate multiple streams: `Stream[TStream, TEvent1]`, `Stream[TStream, TEvent2]`, `Stream[TStream, TEvent3]`, and so on.
- Every stream registered with a given channel uses that channel's same `TStream`. `TStream` is the channel/stream-family type context and contributes to stream and channel schema identity; it is not the event payload type.
- Each stream supplies its own `TEvent`, which determines its typed payload and event schema identity. Event instances remain `Event[TEvent]`.
- Keep stream operations strongly typed for their respective `TEvent`; do not collapse the public API to `Stream[TStream]` or make `EventChannel` non-generic.
- The channel may relate streams with different event types; do not constrain all streams in one channel to share a single `TEvent`.
- Preserve private channel instance identity. The generic type relationship and schema fingerprints are not substitutes for the channel's private runtime UUID.

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

## Current handoff — generic channel/stream type relationship (2026-11-21)

### Repository and verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; work directly on `main`, no branches.
- Current `main` HEAD before this steering update: `f504d93a78fcb1994f447d92f93c3951c0236063` (documentation-only steering checkpoint).
- Latest code SHA with completed CI: `1a498c57dc19493659dabc25400bbddf02bbf83f`; CI run #211 (`44914465680`) completed successfully on Linux and Windows.
- Both platforms passed non-mutating formatting verification, race-enabled tests with coverage, per-function coverage reporting, artifact upload, vet, and build. Both reported 84.7% aggregate statement coverage and matching per-function coverage. No cross-platform coverage discrepancy was found.
- No local Go test execution is claimed. This steering-only change is not CI-tested.
- `docs/storage_disk.md` matches the version-2 six-fingerprint record implementation.

### Agreed type architecture
- The user confirmed the intended relationship:
  ```go
  EventChannel[TStream]
      |
      +-- Stream[TStream, TEvent1] -- Event[TEvent1]
      |
      +-- Stream[TStream, TEvent2] -- Event[TEvent2]
      |
      +-- Stream[TStream, TEvent3] -- Event[TEvent3]
  ```
- One `EventChannel[TStream]` relates many streams. All streams associated with that channel share the same `TStream`; each stream may have a different `TEvent`.
- `TStream` is the stream-family/channel type context and links each stream back to `EventChannel[TStream]`. It is not an event payload type.
- `TEvent` is the event payload type for an individual stream. Events remain `Event[TEvent]`, and subscriptions/publication remain typed to that event type.
- Do not revert to non-generic `EventChannel`, `Stream[TStream]`, or the current `Stream[T]` model. The earlier handoff statement that `EventChannel` must be non-generic and that generic free functions should replace the channel relationship is superseded and incorrect.
- The exact registration/storage mechanism for holding multiple differently typed streams under one generic channel remains to be designed. Preserve strong typing at the public stream API; decide deliberately how the internal registry represents heterogeneous stream instances.
- Fingerprint dependency direction must reflect the type relationship: Event identity is based on `TEvent` and event-level policy; Stream identity incorporates the shared `TStream` context and its `TEvent` dependency; Channel identity incorporates its `TStream` context and the deterministic collection of registered stream schema identities. Exact descriptor composition and effective-fingerprint behavior still require a detailed contract before implementation.

### Existing implementation state (not yet aligned)
- Current `EventChannel` is non-generic, current `Stream[T]` is generic only on the event payload, and current `Event[T]` is generic on the payload. The source therefore does not yet implement the agreed architecture.
- Channel UUID is private; keep it private through the generic API change.
- Runtime schema identities are not yet composed end-to-end across Event, Stream, and Channel. The version-2 disk record already carries six fingerprint fields (Event, Stream, Channel base/effective); runtime objects must eventually supply them.
- Redaction policy is currently supplied per Event. Define how differing policies across events/streams affect Stream and Channel effective fingerprints; do not silently freeze one policy or permit undocumented identity drift.
- Private storage location/ownership and writer lifecycle per logical channel remain unresolved. Do not invent a path, global directory, or public configuration API. Resolve before connecting recorder lifecycle to Publish.
- `SyncEachAppend` remains unsupported pending documented and tested directory-entry durability.

### Next task
1. Design the generic public API and registration contract for `EventChannel[TStream]` owning many `Stream[TStream, TEvent]` instances with different `TEvent` types. Define construction, lookup/registration, duplicate rules, channel close behavior, and how the internal registry safely represents heterogeneous stream types without weakening public typing.
2. Define schema descriptors and deterministic base/effective fingerprint composition for Event, Stream, and Channel, including the shared `TStream` dependency, each `TEvent` dependency, channel stream-set ordering, and per-Event redaction-policy variation.
3. Implement the agreed API and identity composition in `event.go`, `stream.go`, `channel.go`, `store.go`, and `schema.go`, plus affected tests and docs. Preserve stream concurrency/delivery semantics and private UUID identity.
4. Test at least three differently typed streams under one channel, typed publish/subscribe, duplicate registration behavior, deterministic channel fingerprints independent of registration order, dependency changes, redaction-policy behavior, and schema mismatch.
5. Verify the exact code SHA through Linux/Windows CI and compare aggregate plus per-function coverage. Update this handoff after CI analysis. Do not trigger the manually controlled release workflow.

Acceptance: the public API expresses `EventChannel[TStream]` → many `Stream[TStream, TEvent]` → `Event[TEvent]` relationships; one channel supports different event types while preserving typed stream operations; Event/Stream/Channel base/effective identities compose deterministically; channel UUID remains private; the six disk fingerprints are derivable from runtime identities; and exact-SHA Linux/Windows CI passes with documented coverage comparison.

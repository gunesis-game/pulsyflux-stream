# Steering: pulsyflux-stream

## Purpose and scope
Maintain a small, typed, concurrency-safe Go event stream. Event redaction is implemented for the private serialization boundary. Durable storage, indexing, schema cataloging, and replay are internal capabilities to be implemented incrementally while preserving the lightweight live-stream core.

## Ownership boundaries
- **Event:** owns typed payload, event-creation time, redaction policy, payload assignment/validation, and event lifecycle. It does not own JSON serialization or deserialization.
- **Stream:** owns concurrent delivery of complete in-memory events. Never redact during Publish.
- **Channel / registry:** owns logical grouping and in-memory typed stream registration. Channel UUID identity is internal infrastructure, not a public persistence contract.
- **Schema:** `schema.go` mediates typed runtime objects and the system of record. It owns canonical descriptors, the repurposed fingerprint mechanism for one channel-family fingerprint derived solely from `TStream`, identity-resolution contracts, redaction policy/validation/mechanics, and generic JSON encode/decode. Retire the abandoned Event/Stream/Channel Base/Effective fingerprint composition unless an audit proves an independent active consumer; do not add per-type schema files or duplicate serialization/fingerprint logic.
- **Storage:** the system of record is event-record-backed. Append-only event records (which contain private channel/stream UUIDs and the channel-family fingerprint) establish persisted channel and stream existence and provide the evidence used to recover identities. Do not add a separate durable channel/stream catalog merely to persist empty entities. Storage owns segmented disk files, durability, reading/scanning, recovery, and rebuildable indexes; it receives schema identities from the schema/runtime boundary and must not independently derive Go type schemas or decide schema equivalence.
- **Replay:** reads historical records and dispatches through existing stream/channel abstractions. Replay must not be re-recorded by the same recorder by default.

## Identity and schema invariants
- Keep event, stream, and channel instance UUIDs internal. Consumers must not inspect, generate, or persist these IDs.
- Runtime UUID instance identity is distinct from the channel-family fingerprint. Persist both; never substitute one for the other.
- `TStream` is the channel-family schema. Its repurposed deterministic fingerprint from the canonical descriptor resolves channel identity for `NewEventChannel[TStream]`, stable across stream/event registrations and redaction. Stream identity is resolved only within that parent channel; the durable identity is the pair (resolved channel identity, resolved stream identity). Consumers do not supply IDs or choose create/open.
- Channel identity is recovered by scanning/looking up persisted event records for the channel-family fingerprint derived from canonical `TStream`. A matching record establishes the persisted channel identity; if no matching event record exists, the constructor creates a new runtime channel UUID. This empty channel is ephemeral until an event is persisted; no separate durable channel catalog is required.
- Stream identity is recovered from persisted event records only after resolving the parent channel. Within that channel's record scope, the stream schema identity selects the persisted stream UUID. If no matching event record exists, a new runtime stream UUID is created; it remains ephemeral until an event is persisted. The same stream schema in two different channels represents two distinct persisted stream instances.
- Event records themselves establish durable channel and stream existence. Their private UUIDs and channel-family fingerprint enable channel-then-stream recovery; no separate catalog of empty entities. Do not claim transparent recovery is implemented until record-based lookup, identity validation, and constructor recovery are implemented and tested end to end.
- Fingerprint-based record resolution is internal package/storage behaviour. Do not expose public Open/Find/Search operations or require consumers to inspect storage records. The package transparently recovers the existing private channel UUID when a matching event record exists; otherwise it creates a new runtime channel identity, which is not durable until an event is appended.
- Repurpose the existing fingerprint mechanism for one channel-family identity. Remove the six Event/Stream/Channel Base/Effective fingerprints if an audit confirms no other active consumers; do not add a seventh fingerprint. Stream resolution remains channel-scoped.
- SHA-256 fingerprints are deterministic and collision-resistant, not a mathematical guarantee of uniqueness. When resolving a fingerprint, validate the canonical descriptor/identity in the persisted metadata rather than assuming a hash match alone proves equivalence.
- The channel-family fingerprint is a private deterministic SHA-256 hash of the canonical `TStream` descriptor, with explicit format versioning. Never hash raw memory, rely on unstable reflection strings, or use random UUIDs as fingerprints.
- Canonical family descriptors capture exported serialized fields, field types, and JSON tags; omit unexported and `json:"-"` fields; normalize field order by JSON name. Struct/package names are omitted for structural equivalence; named scalar types retain identity. Descriptor changes are versioned.
- The family fingerprint is a channel lookup identity, not migration or semantic-compatibility validation. Current descriptors do not capture custom JSON methods or external codec semantics.
- Use full cryptographic hashes. Do not truncate or force hashes into UUID formatting without a separately justified format.
- One channel-wide persisted sequence defines historical order across all streams in that channel; stream replay filters this global order. Sequence is authoritative. The record timestamp is event-creation time (not disk-write time) and is metadata for filtering/diagnostics, never an ordering substitute.
- Redaction must happen before persistence. Persisted redacted data cannot restore excluded secrets.

## Storage taxonomy and direction
- Keep storage taxonomy explicit: Lifecycle (Open/Close), Write (Append/Flush/Sync), Read (Scan/Read), Lookup (Find/Seek), Replay (Replay/Resume), Maintenance (Recover/Rotate/Compact/Prune/Verify), Metadata (catalog/index).
- Prefer append-only segmented files. Make record framing, checksums, segment rollover, sync policy, and recovery boundaries explicit and versioned.
- Distinguish application-buffer Flush from OS/filesystem Sync durability. Document the durability window for each policy.
- Storage backends handle already-redacted JSON payload bytes and disk I/O; they must not own Go payload types, schema encoding/decoding, or redaction rules. Storage records own persistence metadata (private IDs, sequence, event-creation timestamp, channel-family fingerprint, framing/checksum). Do not duplicate schema type identity or timestamp inside the payload.
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
- `NewEventChannel[TStream]` is the single consumer-facing channel entry point. It transparently recovers a channel UUID from persisted event records using the channel-family fingerprint, or creates a runtime identity when none exists. Empty channels are not durable; consumers cannot choose create/open or supply IDs.
- `New[TStream, TEvent](channel, bufferSize)` remains the stream registration entry point. Internally, it resolves an existing stream using its schema identity within the resolved channel's persisted event records and restores its private stream UUID, or creates a new runtime stream identity when no matching record exists. Empty streams are not durably cataloged. The consumer does not need to know whether a stream has prior events.
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

## Current handoff — fingerprint repurpose for family-backed recovery (2026-11-22)

### Verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`, branch `main`.
- Last verified code SHA: `4c432a2eefc858c0289a0392bea79684059be846`, CI run #217 (`44918459724`) passed Linux and Windows. Aggregate coverage 84.4% on each; 60 functions matched. This remains the last verified code checkpoint. Current changes are documentation-only and not CI-tested.
- Existing generic EventChannel/Stream API and redaction behavior remain.
- Disk record format is v2 with six fingerprints; no implementation changes for the family fingerprint yet. `docs/storage_disk.md` alignment was last checked before this redesign.

### Agreed direction
- Repurpose existing fingerprint machinery for one channel-family fingerprint derived solely from canonical `TStream`, invariant to registered streams/events, redaction, and aggregate channel composition.
- Audit six-fingerprint consumers, then remove unused Event/Stream/Channel Base/Effective composition and record metadata. Do not add a seventh fingerprint.
- Event records alone establish persisted channel/stream existence. Recover channel UUID by family fingerprint; recover stream UUID only within the recovered channel. Empty entities remain ephemeral.
- Keep IDs and recovery metadata internal. No consumer-facing open/find/search or ID-selection API.

### Remaining gaps
- Channel and stream constructor recovery is not implemented; constructors currently generate runtime UUIDs.
- Publish/record lifecycle is not connected to disk append.
- Storage location/ownership and writer lifecycle remain unresolved; do not invent configuration.
- `SyncEachAppend` remains unsupported until directory-entry durability is designed/tested across OSes.
- The six-fingerprint docs and disk format docs need audit/reconciliation.

### Next task
1. Audit all six fingerprint usages in schema code, disk record encoding/validation, tests, and docs.
2. Implement family fingerprint from canonical `TStream` by repurposing existing fingerprint code.
3. Remove unused six-fingerprint composition and record fields; update encoding/validation, fixtures, tests, `docs/storage_disk.md`, `docs/schema_identity.md`, and `docs/record_scope.md` coherently. No seventh fingerprint or unnecessary version bump.
4. Test family stability across registered stream/event types and redaction policies, distinct families, canonical descriptors, record encode/decode, and recovery.
5. Preserve redaction/payload boundaries; keep `SyncEachAppend` unsupported until durability contract is agreed.
6. Verify code changes against exact-SHA Linux/Windows CI and compare aggregate/per-function coverage. Do not trigger release CI.

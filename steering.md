# Steering: pulsyflux-stream

## Purpose and scope
Maintain a small, typed, concurrency-safe Go event stream. Event redaction is implemented for the private serialization boundary. Durable storage, indexing, schema cataloging, and replay are internal capabilities to be implemented incrementally while preserving the lightweight live-stream core.

## Ownership boundaries
- **Event:** owns typed payload, derived event type identity, internal timestamp, redaction policy, payload assignment/validation, and private serialization/deserialization.
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

## Current handoff — reliable cross-platform directory-entry durability (2026-11-21)

### Repository and verified baseline
- Repository: `gunesis-game/pulsyflux-stream`; branch: `main` only.
- Current `main` HEAD at handoff preparation: `a48784052ea499e8d5e5c295dd60a5f90b1f139c` (documentation-only coverage-comparison update).
- Latest verified code/workflow commit: `b54bb44bb498a644acfc0f051ce5da21026e4e96`.
- CI run [44899535090](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44899535090), run #162, completed successfully for exact code SHA `b54bb44bb498a644acfc0f051ce5da21026e4e96`. Cleanup and both OS matrix jobs passed formatting, race-enabled tests with coverage profiles/reports, artifact uploads, vet, and build.
- Coverage comparison from that same run: Linux **81.4%**, Windows **80.9%** (0.5 percentage-point difference). Shared functions in channel.go, event.go, schema.go, store.go, and stream.go matched. Platform-specific directory sync accounts for the expected helper difference: Linux `syncSegmentDirectory` 75.0%; Windows helper 0.0% because strict mode is rejected there. Shared storage functions with differing coverage include `openDiskWriter` (67.9% Linux / 69.8% Windows), `newSegment` (88.9% / 77.8%), `Append` (85.7% / 81.0%), and `syncLocked` (100% / 66.7%). The comparison is per-function, not a statement-level profile diff. Do not force equal aggregate percentages with artificial tests.
- Markdown-only steering updates do not trigger CI. No local Go test execution is claimed.

### Active problem and strict contract
- User requires the disk backend to behave consistently on Linux and Windows; do not accept a weaker Windows durability guarantee.
- Under `SyncEachAppend`, a successful append must not be acknowledged until both file contents and the segment's directory entry are covered by a reliable durability sequence.
- Linux currently implements parent-directory sync in `storage_disk_sync_linux.go`.
- Windows currently returns `errDirectorySyncUnsupported` from `storage_disk_sync_windows.go`; writer initialization rejects `SyncEachAppend` before opening or creating segment files. This fail-closed behavior avoids falsely claiming strict durability but leaves the option unavailable on Windows.
- Do not silently fall back to file-only sync, and do not use an unverified Windows directory-handle/FlushFileBuffers technique. A Windows implementation must be supported by authoritative platform documentation and tests that exercise the real behavior.
- Supported disk-backend OS scope is Linux and Windows. Preserve the strict contract; do not change it to asymmetric durability or file-only syncing without explicit user approval.

### Next task and acceptance criteria
1. Research a supported Windows mechanism for making newly created segment directory entries durable, using authoritative Microsoft documentation and relevant Go/Windows API behavior. Distinguish documented guarantees from assumptions; do not treat a successful API call alone as proof of crash durability.
2. If a reliable mechanism exists, implement it behind the Windows-specific helper with focused tests. If no supported mechanism exists, report the evidence and propose the narrowest viable architecture that still preserves one cross-platform contract; do not weaken the contract silently.
3. Review Linux and Windows ordering for segment creation, directory sync, file sync, reopen, and rotation. Both platforms must acknowledge append only after the same required durability conditions have succeeded.
4. Update `docs/storage_disk.md` and its diagrams in the same commit as any storage implementation change.
5. Add tests for success, unsupported/error cases, segment creation, reopen, rotation, and sticky failure behavior. Tests must prove no append is acknowledged after a required sync fails.
6. Push directly to `main`; verify the exact CI SHA and all cleanup/Linux/Windows jobs and steps. Compare aggregate and per-function coverage after that run; inspect artifacts if differences are not explained.
7. Update this handoff in place with verified results and the next task. Remove this handoff once the cross-platform durability task is successfully resolved and verified.

### Non-goals until separately justified
- Do not start indexing until a concrete Find/Seek requirement and contract exist.
- Do not add a public storage API, SQL, or distributed infrastructure.
- Do not change the existing public event/stream/channel API or expose internal IDs/fingerprints.

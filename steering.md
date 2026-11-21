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
- At the start of the 2026-11-21 continuation, `main` was at `bd52f6d03ca68f88513defa260f9b6f1908a657d`. The storage hardening commits are recorded in the follow-on implementation checkpoint below. Confirm HEAD and CI afresh.
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


### Follow-on implementation checkpoint (2026-11-21)
- Committed directly to `main`: `3e26b78` (incremental reader and sticky writer I/O failure handling), `ccae62b` (reader corruption and segment-name tests), `292e2f5` (reject missing segment numbers), and `ed250cf` (gap tests).
- Reader now decodes one frame per `Next`, holds only the active segment file, closes segments at EOF and on terminal errors, and rejects incomplete tails, complete-frame corruption, and non-increasing sequence values across all segment boundaries.
- Shared segment discovery validates canonical `segment-%06d.pfs` names and requires a contiguous sequence beginning at segment 1. This is appropriate while storage is append-only and no pruning/compaction exists; revisit deliberately if retention is later introduced.
- Writer I/O failures become sticky: after a failed append/flush/sync/rotation, later writes fail rather than continuing from uncertain on-disk state.
- No index work was started. Reader reliability remains unverified until the complete CI run passes.
- Verification limitation during this session: the available GitHub workflow-run lookup only returns pull-request-triggered runs; commit combined-status lookup returned no statuses for the new push commits. A local checkout could not be cloned because the execution environment could not resolve github.com. Do not report CI green from these empty results; obtain the push-triggered Actions run and confirm formatting, race tests/coverage, vet, and build before indexing.


## Session handoff — verified storage reliability checkpoint (2026-11-21)

### Authoritative repository state
- Repository: `gunesis-game/pulsyflux-stream`; continue directly on `main`.
- Verified `main` HEAD: `1d463b60bbfe749de8b06f2ad3ff17fed6fe7de8`.
- The older handoff above contains stale/contradictory checkpoint details and CI uncertainty. This section supersedes those details for the current state.
- Latest verified CI run: [44892436892](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44892436892), completed with conclusion `success` for the exact HEAD above. The single Test and build job completed successfully, including Go formatting, formatting verification, `go test -race -cover ./...`, `go vet ./...`, and `go build ./...`.
- The preceding run 44892380803 failed because the new sticky-writer test used the default buffered writer, so the injected failure was not reached during Append. The test was corrected to use `bufio.NewWriterSize(..., 1)`; the corrected test passed in run 44892436892. Do not treat the preceding failure as an unresolved product-code failure.
- CI workflow policy already implemented: Markdown-only changes (including new Markdown files) must not trigger CI; mixed changes containing relevant non-Markdown files should trigger CI; manual workflow dispatch remains available. Preserve this behavior. Do not change the workflow unless specifically requested.

### Latest reliability tests added
- Commit `a041f232a8638e85037ee512544d84dcb7c54e80`: added `TestDiskReaderStopsAtCorruptionInMiddleSegment`. It verifies that after reading valid records, the reader fails closed on a complete checksum-corrupt frame in a middle segment, does not skip to a later segment, and retains the terminal error on subsequent `Next()` calls. CI run 44892340353 passed.
- Commit `e015e9c761d1cd38db8249e1c325554a6c591600`: added `TestDiskWriterAppendFailureIsSticky` and a failing writer helper. Its first CI run exposed the buffering issue described above.
- Commit `1d463b60bbfe749de8b06f2ad3ff17fed6fe7de8`: corrected the injected writer to use a one-byte bufio buffer. This is the current HEAD and its full CI run passed.
- Earlier verified test `TestDiskWriterReopenAcrossSegmentsReaderContinuity` covers append, writer close/reopen, additional appends across segments, and sequential reader continuity through all six records.

### Current storage behavior and constraints
- Keep the disk reader incremental: decode one framed record per `Next()`, retain only the active segment file, close it at EOF and on terminal errors, and preserve terminal errors.
- Keep segment discovery strict: canonical `segment-%06d.pfs` names and contiguous numbering starting at 1. This assumption is acceptable for append-only storage without pruning/compaction; revisit only as part of an explicit retention design.
- Reader must reject incomplete tails, complete-frame corruption, and non-increasing sequence values across segment boundaries. It must not skip middle corruption.
- Writer I/O failures are sticky; after a failed append/flush/sync/rotation, later appends must return the stored failure rather than continue from uncertain disk state.
- Writer reopen scans segments to recover sequence state, rejects incomplete tails in non-active segments, and truncates an incomplete tail only in the active final segment.
- Important remaining memory concern: `scanDiskRecords`, used during writer recovery, currently accumulates decoded records for a segment in a `[]diskRecord`. The public-facing reader is incremental, but recovery scanning may still use memory proportional to segment contents. Review whether recovery can be made streaming without weakening validation/recovery semantics.
- Do not start index implementation until the disk reader and recovery path have been deliberately reviewed and considered reliable. No index implementation has been started.
- Keep public API and identity boundaries unchanged: no public storage API, no public storage-record model, no exposure of internal UUIDs or schema fingerprints. Redact before persistence. Sequence numbers define persisted order; timestamps are metadata.

### Next session — recommended work order
1. Verify current `main` HEAD and the latest completed CI run before editing.
2. Inspect the current `storage_disk.go` and `storage_disk_test.go` rather than relying solely on this handoff.
3. Review recovery scanning memory behavior and error-path semantics. Consider converting recovery scan to a streaming scan that tracks only the state needed (last sequence, valid byte offset, incomplete-tail status, and any required validation), while preserving complete-frame corruption detection and monotonic sequence checks.
4. Add focused tests for any changed recovery behavior, including large/multi-record segments, complete corruption, incomplete active tail truncation, and incomplete non-active segment rejection.
5. Continue reviewing writer failure paths (flush, sync, segment rotation, close) with injected I/O failures where practical.
6. Commit directly to `main`, wait for CI, and inspect the actual run and every job step before reporting success.
7. Only after reader and recovery reliability are established, discuss a minimal rebuildable index design. Do not introduce SQL, distributed infrastructure, or a public storage API prematurely.

### Session operating rules
- Do not edit Markdown files except when explicitly asked. This handoff was explicitly requested; ordinary implementation commits should avoid Markdown-only edits.
- Markdown-only changes must not trigger CI. Do not claim a CI run occurred for this documentation-only update.
- Do not claim local Go tests were run unless a local checkout and execution actually occurred. GitHub Actions is the verified source for the latest test/build results.

## Mandatory iteration, analysis, and session handoff maintenance

This section is a required operating procedure for every AI implementation session. The purpose is to make the repository, especially this file, the durable source of truth so work can be continued in a fresh AI session without relying on chat history.

### Required iteration sequence
1. Inspect the current `main` HEAD, recent commits, working implementation, and relevant tests before making changes.
2. Implement one coherent change or a clearly scoped set of related changes.
3. Commit and push directly to `main`, unless the user explicitly requests another workflow.
4. Identify the GitHub Actions run for the exact pushed commit. Wait for it to finish and inspect the run conclusion and individual job/step results and logs. Never infer success from a missing, queued, partial, or unrelated run.
5. If CI fails, analyze the failure and its root cause. Fix the underlying issue rather than suppressing, bypassing, or weakening the check. Commit and push the correction, then verify the new run against the new exact HEAD. Preserve the failed run and its cause in the handoff as useful history.
6. After the code review and CI analysis, update this `steering.md` in a separate documentation commit with the current verified state and next-session handoff. Do this whether CI passed or failed; clearly distinguish a resolved failure from an outstanding failure. If a correction is still in progress or CI has not completed, record that explicitly rather than presenting the iteration as complete.
7. Verify the steering update is committed to `main`. A Markdown-only steering update follows repository policy and must not trigger CI; do not claim that it did or that a CI run passed for that documentation-only commit.

### Mandatory post-CI analysis
A CI result is not the end of an iteration. After inspecting CI, review the implementation and test coverage and deliberately identify:
- Any correctness, reliability, security, performance, maintainability, or test-coverage gaps revealed by the change or review.
- Any missing edge cases, untested failure paths, incomplete design decisions, technical debt, or assumptions that need validation.
- Any substantial follow-on work that should be planned before implementation, including dependencies, scope boundaries, risks, and decisions that require user input.
- Whether the next task is safe to start now, should be split into smaller tasks, or must be deferred until a prerequisite is reviewed or completed.

Do not invent gaps merely to fill the handoff. If the review finds no material gap, state that the review found none and explain briefly what was checked. Distinguish verified facts from hypotheses and recommendations.

### Required contents of every session handoff
Update the latest authoritative checkpoint in this file after every implementation iteration and before ending or handing off a session. Include:
- Repository and branch, exact latest HEAD commit SHA, and a concise description of the implementation changes.
- The relevant CI run URL and run ID, exact commit SHA tested, final status/conclusion, and the meaningful job/step results. If no run exists or it is pending, state that plainly.
- Any CI failures, their diagnosed causes, the corrective commits, and whether the correction has been verified by a subsequent run.
- A concise analysis of the current implementation state, including what is complete and what remains unverified.
- Discovered gaps, known limitations, deferred decisions, and risks. Mark each as confirmed or suspected where that distinction matters.
- The recommended next task (or explicit statement that no immediate task is recommended), why it is next, and a practical implementation plan.
- Prerequisites and dependencies, files/components likely involved, tests or acceptance criteria needed, and any constraints or work that must not be started yet.
- Any architecture documentation reviewed or updated, including whether `docs/storage_disk.md` remains consistent with the implementation.

Keep the newest checkpoint authoritative. Preserve important historical decisions, constraints, and unresolved issues, but clearly mark older checkpoints as superseded when later facts replace them. Remove stale instructions that could mislead a new session only when their relevant history and rationale are preserved.

### Session interruption and fresh-session rule
If the AI session may end, context may be exhausted, or work is being handed to another session, update this file before stopping. The handoff must be sufficient for a new AI session to:
1. Verify the repository state and latest CI result independently.
2. Understand what was changed and why.
3. Know what was found during post-change analysis.
4. Start the recommended next task without asking the user to reconstruct prior chat context.

If implementation is mid-flight, record the exact stopping point, uncommitted or unpushed work if any, pending checks, known risks, and the safest next action. Never describe uncommitted work as pushed, or unverified work as tested.

**Completion rule:** An implementation iteration is not fully handed off until its code changes, verified (or accurately reported pending/failed) CI outcome, post-CI analysis, and actionable next-session checkpoint are recorded in this repository.

## Architecture documentation maintenance — mandatory AI instruction

- Treat [`docs/storage_disk.md`](docs/storage_disk.md) as the canonical, maintained architecture guide for `storage_disk.go`. It is an implementation document with detailed Mermaid diagrams, not optional supplementary prose.
- Whenever a change modifies `storage_disk.go` behavior, data structures, record framing/encoding, segment naming or lifecycle, recovery semantics, writer durability/failure handling, reader behavior, or storage invariants, review and update `docs/storage_disk.md` in the **same commit** so its explanations, tables, source map, and diagrams describe the resulting implementation.
- Update the relevant diagrams as part of that change; do not merely append a note saying behavior changed. Keep diagrams readable and split complex flows into focused diagrams rather than growing one overloaded chart.
- If a storage change introduces, removes, or renames a type/function or changes an invariant, update the component map, source map, and invariant sections where applicable.
- Before committing, cross-check every documented claim against the final source and tests. Do not document intended or proposed behavior as if it is implemented. If behavior is deliberately deferred or remains a known limitation, label it clearly as such.
- If a change outside `storage_disk.go` materially changes the storage contract or its assumptions (for example redaction-before-persistence, schema identity, or replay integration), assess whether the architecture guide needs a corresponding update and make it when relevant.
- Do not edit `docs/storage_disk.md` for unrelated changes. Preserve the repository's CI policy: Markdown-only changes do not trigger CI; when a storage implementation change requires both Go and Markdown updates, the Go changes must trigger the normal CI checks.
- In handoffs and completion summaries, mention whether the storage architecture guide was updated or reviewed. Never leave a known mismatch between the implementation and its architecture diagrams for a later session to discover.

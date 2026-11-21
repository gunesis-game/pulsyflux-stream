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

## Session handoff — recovery review and CI formatting gap (2026-11-21)

### Authoritative repository and verification state
- Repository: `gunesis-game/pulsyflux-stream`; continue directly on `main`.
- Current `main` HEAD: `7c6336ba24b30b4a87c9876895727973422c32a0` (`docs: require post-CI analysis and durable session handoffs`). This is a Markdown-only steering update and, per repository workflow policy, does not trigger CI.
- Latest relevant completed CI run: [44895528595](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44895528595), run #148, for exact code commit `ca5945b4426d5bccc402679b8d9ea030ddff8f09`. Run completed successfully. Job `Test and build` passed formatting step(s), `go test -race -cover ./...` (81.9% statement coverage), `go vet ./...`, and `go build ./...`.
- The preceding run [44895519915](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44895519915) failed because `storage_disk_test.go` lacked the `fmt` import. Commit `ca5945b` added it; run 44895528595 verified the correction.
- No CI run is expected for the current documentation-only HEAD. Do not describe the current docs commit as independently CI-tested.

### Completed recovery-path review
- Reviewed current `storage_disk.go`, `storage_disk_test.go`, and `docs/storage_disk.md`.
- The earlier concern that writer startup recovery accumulated every decoded record is already resolved in the implementation: `scanDiskRecords` processes one frame at a time and retains only the last sequence, valid byte offset, and incomplete-tail state. Per-record temporary memory is bounded by the maximum frame size; memory does not grow with record count.
- `TestScanDiskRecordsTracksSequenceWithBoundedState` scans 4,096 records and verifies the final sequence and offset.
- Recovery tests cover sequence regression, active-final-segment incomplete-tail truncation, rejection of incomplete tails in non-active segments, and rejection of complete corruption in the active segment.
- Reader tests cover incomplete tails, complete corruption, sequence regression across segments, and fail-closed behavior on corruption in a middle segment.
- `docs/storage_disk.md` already describes streaming recovery, its memory profile, and the recovery/reader distinction. No source or architecture-document change was needed in this review.
- Conclusion: do not rewrite the recovery scanner; the prior memory concern is resolved. Do not start an index without a concrete Find/Seek requirement or agreed lookup contract.

### Newly discovered confirmed CI gap — formatting check mutates the checkout
- Current `.github/workflows/ci.yml` runs `gofmt -w $(find . -name '*.go')` before running `gofmt -l .`.
- This means CI formats the runner's working tree before checking it. The formatting verification can therefore pass even when the committed Go files are not gofmt-clean. Tests, vet, and build then run against the runner's formatted working tree rather than necessarily the exact committed Go source.
- This undermines the intended formatting gate and must be corrected. Do not treat the existing formatting step as proof that committed files were already formatted.
- The repository currently has ten tracked Go files: `channel.go`, `event.go`, `event_test.go`, `schema.go`, `schema_test.go`, `storage_disk.go`, `storage_disk_test.go`, `store.go`, `stream.go`, and `stream_test.go`.

### Next task — make formatting verification non-mutating
1. Obtain a working copy of all ten tracked Go files and run `gofmt` on them locally or in a controlled formatting step; commit the formatted Go source so the repository itself is clean.
2. Change `.github/workflows/ci.yml` to remove the mutating `gofmt -w` step. Keep a non-mutating check using `gofmt -l` that prints every offending filename and fails if the list is non-empty.
3. Ensure formatting verification runs against the exact committed tree before tests, vet, and build. Do not allow CI to silently repair files before checking them.
4. Commit the Go formatting and workflow correction directly to `main`; wait for the push-triggered CI run and verify its exact `head_sha`, each job step, and logs.
5. Update this handoff after the run, recording the exact result. If formatting fails, fix the listed files and repeat the commit/CI verification sequence.
6. Preserve the workflow's existing Markdown path-ignore behavior and manual dispatch. Do not weaken race tests, vet, or build checks.

### Other known constraints and deferred work
- Parent-directory durability after segment creation remains an acknowledged design limitation. Do not add directory syncing until supported operating systems and the required crash-durability contract are explicitly defined.
- No persistent index or public storage API is implemented. Keep indexing deferred until a concrete lookup use case and contract justify it.
- No local test execution was performed during this review. The latest confirmed Go verification is the GitHub Actions run listed above.

## Session handoff — non-mutating formatting gate exposed unformatted files (2026-11-21)

### Latest implementation and CI state
- Branch: `main`.
- Latest implementation commit: `f0270afeb0ba6d7002ab7624fbb2caacb83b9529` — `ci: verify Go formatting without modifying checkout`.
- Changed `.github/workflows/ci.yml` to remove the `gofmt -w` mutation step. The remaining formatting gate uses `gofmt -l .`, prints offending filenames, and fails when any are found. Existing test, vet, build, permissions, Go version, and Markdown path-ignore behavior remain unchanged.
- CI run [44896560030](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44896560030) tested exact commit `f0270afeb0ba6d7002ab7624fbb2caacb83b9529` and completed with failure.
- Job `Test and build` failed at `Verify formatting`. Logs explicitly listed:
  - `storage_disk.go`
  - `storage_disk_test.go`
- `Run tests`, `Run vet`, and `Build package` were skipped because the formatting gate correctly failed. This is not a test/build failure; those checks have not yet been rerun against the corrected committed source.
- This failure confirms the previously suspected CI gap: the old mutating step had been masking formatting drift in these two committed files.

### Current state and blocker
- The workflow now verifies the committed tree without silently repairing it.
- The two named files still need to be run through `gofmt` and committed as formatted source.
- No source files have been changed in this iteration. No claim is made that tests, vet, or build passed for commit `f0270af`.
- The current execution environment has no working network route for a local `git clone` (DNS resolution for github.com failed). GitHub file read/write access is available, but the files have not yet been safely formatted in a local checkout. Do not manually approximate gofmt output or reintroduce a mutating CI step just to obtain a green result.

### Next task — format the two identified files and re-run the full gate
1. Obtain a working checkout of `main` at `f0270afeb0ba6d7002ab7624fbb2caacb83b9529`, or another reliable way to run the repository's Go 1.27.4 `gofmt` formatter on the exact contents.
2. Run `gofmt -w storage_disk.go storage_disk_test.go` only; avoid unrelated source changes.
3. Inspect the diff to confirm it is formatting-only. Preserve all behavior and tests.
4. Commit and push the two formatted files to `main`.
5. Verify the new CI run corresponds to the exact new HEAD and passes the non-mutating formatting check, `go test -race -cover ./...`, `go vet ./...`, and `go build ./...`. Record the actual coverage reported by CI.
6. If any step fails, diagnose and correct the root cause, then repeat verification. Do not restore `gofmt -w` to the CI verification workflow.
7. Update this steering handoff after the result. Only after the source-format commit and successful CI should the formatting-gate task be marked complete.

### Confirmed observations and constraints
- The non-mutating formatting gate found exactly two files, as listed in the run logs. The other tracked Go files were not listed by `gofmt -l .`.
- Recovery review remains complete: the disk scanner processes frames incrementally; recovery tests cover incomplete active tails, non-active tails, complete corruption, and sequence regression. No recovery rewrite is warranted from the current evidence.
- Keep indexing deferred until a concrete lookup/seek requirement and contract are defined.
- Parent-directory sync durability remains deferred until supported platforms and the crash-durability contract are defined.

## Session handoff — non-mutating formatting gate fixed and verified (2026-11-21)

### Authoritative repository and CI state
- Repository: `gunesis-game/pulsyflux-stream`; branch: `main`.
- Latest implementation HEAD: `19048e3843f623bcb034860a647c2ac4b680a9c9` — `style: format disk storage tests`.
- Formatting commits in this iteration:
  - `bfd168bf284497751d5362d759ed1bf1f2777ac2` — formatted `storage_disk.go`.
  - `19048e3843f623bcb034860a647c2ac4b680a9c9` — formatted `storage_disk_test.go`.
- The workflow change removing the mutating `gofmt -w` step remains in commit `f0270afeb0ba6d7002ab7624fbb2caacb83b9529`. The current formatting gate runs `gofmt -l .`, prints offending filenames, and fails without changing the checkout.
- CI run [44897481661](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44897481661) completed successfully for the exact latest implementation HEAD `19048e3843f623bcb034860a647c2ac4b680a9c9`.
- Job `Test and build` passed all steps: checkout, Go setup, non-mutating formatting verification, `go test -race -cover ./...`, `go vet ./...`, and `go build ./...`.
- Test result: package passed with **81.9% statement coverage**. Vet and build completed successfully.
- The first formatting commit `bfd168b` triggered run [44897325454](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44897325454), which failed only at `Verify formatting` because `storage_disk_test.go` still needed formatting. Tests, vet, and build were skipped in that run. The second formatting commit fixed the remaining file; run 44897481661 verified the final tree.
- Earlier run [44896560030](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44896560030) had exposed both files (`storage_disk.go` and `storage_disk_test.go`) before either was formatted. That issue is now resolved.
- No CI run is expected for this Markdown-only steering checkpoint commit. Do not claim that CI tested the steering update itself.

### Changes and post-CI analysis
- Formatted only `storage_disk.go` and `storage_disk_test.go`; no intended behavior, public API, test assertions, or storage invariants were changed.
- The formatting check now validates the committed tree rather than repairing it in the runner. This closes the false-green formatting-gate gap.
- CI verifies the resulting source using the repository's configured Go 1.27.x toolchain. A local Go 1.23.2 `gofmt` binary was used to prepare the formatting, and the CI formatting check confirms the final committed files are accepted by the configured toolchain.
- No local Go tests, vet, or build were claimed. GitHub Actions is the verification source.
- Reviewed `docs/storage_disk.md`; it remains consistent because this iteration was formatting-only. No architecture-document update was needed.- No new functional or test-coverage gap was identified by this formatting-only iteration. The CI coverage result remains 81.9%; this is a measurement, not by itself evidence that every edge case is covered.

### Next task — define the disk segment directory-durability contract before implementation
- Known limitation: syncing a segment file does not necessarily make a newly created directory entry durable across sudden power loss. The current writer sync policy should not be described as guaranteeing durable segment creation until this is addressed.
- First decide and document the required guarantees:
- Current repository evidence: `README.md` declares Go 1.27 or newer but does not declare supported operating systems; the CI workflow currently runs on `ubuntu-latest` only. This does not establish that Linux is the only supported target. The supported OS/filesystem matrix remains an explicit open decision.
  1. Which operating systems and filesystems are in scope?
  2. Does a successful append under `SyncEachAppend` need to survive process termination only, kernel crash, or sudden power loss?
  3. Must the creation of a new segment filename be durable before an append to that segment can be acknowledged?
  4. What behavior is expected when directory syncing is unsupported or fails?
- Inspect platform-specific directory-sync behavior and Go APIs for the supported targets. Do not assume that opening and syncing a directory works identically on every OS.
- Write down the agreed contract and failure semantics in `docs/storage_disk.md` before implementing platform-specific code. If the platform support or durability requirement is not clear, ask the user for that decision rather than guessing.
- After the contract is explicit, design the smallest platform-appropriate helper and tests. Keep platform-specific code isolated; preserve append ordering, sticky I/O failure behavior, and the existing file-sync semantics.
- Update the architecture guide and diagrams in the same commit as any storage behavior change. Then run and verify the complete CI suite against the exact pushed commit.
- Keep persistent indexing and public storage APIs deferred until a concrete lookup/seek requirement and contract are defined.

### Handoff instructions for the next AI session
1. Start by verifying `main` HEAD and the latest CI run independently.
2. Treat `19048e3843f623bcb034860a647c2ac4b680a9c9` and CI run `44897481661` as the latest verified implementation checkpoint.
3. Do not repeat the completed formatting-gate work unless a later change reintroduces formatting drift.
4. Continue with the directory-durability contract decision above; do not implement speculative OS-specific syncing before the supported platforms and crash-durability semantics are defined.

## Session handoff — platform scope selected for disk durability (2026-11-21)

### Current repository state
- Repository: gunesis-game/pulsyflux-stream; branch: main.
- Current HEAD before this handoff: d0b9f2fd560e447b5afc6e43f15d48122edc0d64.
- Latest verified implementation remains 19048e3843f623bcb034860a647c2ac4b680a9c9, with successful CI run 44897481661 (formatting, race tests, vet, build; 81.9% coverage).
- This handoff is documentation-only; do not claim it was tested by CI.

### User decision
- Supported operating systems for the disk backend: **Linux and Windows**.
- This resolves the OS scope question. It does not yet define whether SyncEachAppend must provide equivalent sudden-power-loss durability on both systems.

### Platform research and design constraints
- Linux fsync(2) documentation states that syncing a file does not necessarily persist its containing directory entry; an explicit fsync on the parent directory is required for that. See https://man7.org/linux/man-pages/man2/fsync.2.html and https://www.man7.org/linux/man-pages/man2/fdatasync.2.html.
- Microsoft documents that opening a directory requires CreateFile with FILE_FLAG_BACKUP_SEMANTICS: https://learn.microsoft.com/en-us/windows/win32/fileio/obtaining-a-handle-to-a-directory and https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew.
- Microsoft documents that FlushFileBuffers requires GENERIC_WRITE access on the handle: https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers.
- The reviewed Windows documentation does not establish a portable, supported equivalent to Linux parent-directory fsync for persisting a newly created segment directory entry. Do not assume that opening a directory and calling os.File.Sync or FlushFileBuffers provides the Linux guarantee.
- Current implementation creates segment files with os.OpenFile(..., O_CREATE|O_EXCL|O_WRONLY, ...); SyncEachAppend flushes the buffered writer and calls w.file.Sync(). newSegment does not sync the parent directory. Rotation flushes/closes the prior segment before creating the next.
- Current CI runs only on Ubuntu. A Linux+Windows support commitment should eventually be reflected in a CI matrix, with race tests run where the Go toolchain/platform supports them and all supported platforms running formatting, tests, vet, and build as appropriate.
- No implementation or CI changes have been made for this durability task yet. docs/storage_disk.md has not yet been changed; it must be updated with the agreed cross-platform guarantee before platform-specific implementation.

### Required user decision before coding
Present this explicit trade-off:
1. **Asymmetric guarantee:** Linux syncs the parent directory after segment creation and can claim the documented file-plus-directory sync sequence (subject to filesystem/device honoring sync). Windows syncs file contents but does not claim that a newly created segment name survives sudden power loss. Windows remains supported with a weaker namespace-durability guarantee.
2. **Strict guarantee:** SyncEachAppend must not report success on Windows unless a reliable, supported directory-entry durability mechanism is established and tested. If no such mechanism is available, return an explicit unsupported/error result rather than silently weakening the guarantee.

Ask the user to choose one before implementing. Do not infer this policy from the OS list alone.

### Next task
1. Obtain the user's choice between asymmetric durability and strict cross-platform durability.
2. Update docs/storage_disk.md first with the selected contract, limitations, failure semantics, and updated writer/rotation/sync diagrams.
3. Implement platform-specific directory synchronization only where the contract and platform APIs support it. Keep platform code in OS-suffixed files and test Linux and Windows behavior; do not introduce unverified Windows directory flushing.
4. Add Linux and Windows to the CI matrix. Preserve the non-mutating gofmt gate, race test on supported runners, vet, and build.
5. Run CI and verify exact commit SHA, each matrix job/step, and logs.
6. Update this steering handoff after CI with post-CI analysis, any gaps, and the next actionable task.

### Fresh-session instructions
- Verify current main HEAD and latest CI before proceeding.
- Treat the user decision “Linux and Windows” as settled.
- The remaining blocker is the cross-platform durability contract, not the OS support list.
- Do not change code until the user chooses the asymmetric or strict guarantee above.


## Session handoff — strict cross-platform durability implemented and CI verified (2026-11-21)

This section supersedes the earlier pending-choice handoff above. The user selected **option 2: strict cross-platform durability**.

### Repository and verification checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; branch: `main`.
- Exact verified HEAD: `e0a2e6f501e32c5c583e99a320dc0c8e804ac0b5`.
- CI run: [44898090996](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44898090996).
- CI run completed successfully with `head_sha=e0a2e6f501e32c5c583e99a320dc0c8e804ac0b5`.
- Linux job `Test and build (ubuntu-latest)`: formatting passed; `go test -race -cover ./...` passed (81.4% coverage); `go vet ./...` passed; `go build ./...` passed.
- Windows job `Test and build (windows-latest)`: formatting passed; `go test -cover ./...` passed (80.9% coverage); `go vet ./...` passed; `go build ./...` passed.
- The race detector is run on Linux only. Windows tests still run normally without `-race`.
- Earlier matrix runs 44897984316 and 44898009550 failed only at Windows formatting verification: checkout line endings caused gofmt to flag all Go files. The root cause was fixed with `.gitattributes` (`*.go text eol=lf`); run 44898090996 verifies the fix on both platforms.

### User-selected contract and implementation
- Supported disk-backend operating systems: Linux and Windows.
- Contract: `SyncEachAppend` must not acknowledge an append unless both file contents and the segment directory entry are covered by the durability sequence.
- Linux implementation is in `storage_disk_sync_linux.go`: it opens the segment directory and calls `os.File.Sync()`. It syncs the directory after creating a segment and when reopening an existing writer with `SyncEachAppend` enabled. Append then flushes and syncs the segment file. A directory-sync failure fails writer initialization or the append/rotation path; append-time failures are sticky.
- Windows implementation is in `storage_disk_sync_windows.go`: the platform capability check returns `errDirectorySyncUnsupported`. `openDiskWriter(..., SyncEachAppend: true)` rejects the mode before opening or creating segment files. There is no silent file-only fallback and no unverified directory-flush hack.
- Other OSes use `storage_disk_sync_other.go` and reject strict `SyncEachAppend` until a verified platform implementation is added.
- The explicit error is `errDirectorySyncUnsupported` in `storage_disk.go`.
- `storage_disk_test.go` checks Windows gets the explicit unsupported error and Linux retains the sync-across-rotation test.
- `docs/storage_disk.md` now documents the strict contract, Linux and Windows behavior, error semantics, platform helper files, and the provisioning boundary.
- The storage directory itself must be durably provisioned by the caller. Linux syncs segment entries within that directory; it does not recursively sync newly created ancestor directories. This limitation is explicit in the architecture guide and should remain visible unless the implementation is expanded to durably create the directory hierarchy.

### Post-CI analysis and remaining gaps
- Both supported OS matrix jobs pass the formatting, test, vet, and build checks. The Windows-specific rejection behavior is now exercised in CI.
- The first CI attempt exposed line-ending normalization rather than a Go code failure; `.gitattributes` resolved it without weakening the non-mutating formatting gate.
- No local test or local gofmt claim is made; verification is from the exact GitHub Actions run above.
- Linux directory fsync can still fail on filesystems that do not support the operation or on storage stacks that do not honor flush guarantees. Those errors fail closed; the implementation cannot override hardware/filesystem guarantees.
- Windows does not currently support `SyncEachAppend` under this strict contract. This is intentional until a supported, reliable, tested directory-entry durability mechanism is established.
- The storage directory/ancestor provisioning boundary is the main durability scope limitation left to decide if broader guarantees are required.

### Next actionable work
1. Consider whether the storage backend should itself durably create and sync a missing storage-directory hierarchy, rather than requiring caller provisioning. Do not broaden this silently; define the exact hierarchy-creation contract and add platform tests first.
2. Keep Windows `SyncEachAppend` fail-closed unless a documented and tested Windows directory-entry durability mechanism becomes available.
3. Any further storage behavior change must update `docs/storage_disk.md` in the same code iteration, run the Linux+Windows matrix, inspect exact job steps/logs, and then add a fresh Markdown-only handoff update.
4. Before further work, independently verify current `main` HEAD and latest CI; do not assume this handoff remains the newest state.


## Session handoff — CI run retention, supersession, and cross-platform race tests (2026-11-21)

### Repository and verified checkpoint
- Repository: `gunesis-game/pulsyflux-stream`; branch: `main` only. The user confirmed there will be no working branches.
- CI workflow implementation commit: `e34aa2cc60ff6c1fcd994e3e5eaf5851e80602dc` (`ci: retain latest runs and cancel superseded workflows`).
- Exact verified CI run: [44898579893](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44898579893), run number 160, completed with conclusion `success` for the exact implementation commit above.
- The prior strict-durability handoff remains the latest storage implementation checkpoint; this CI change did not modify storage code or its documentation.

### CI workflow changes
- `.github/workflows/ci.yml` now triggers on pushes to `main` and manual `workflow_dispatch` only. The pull-request trigger was removed to reflect the user's main-only workflow.
- A single workflow-wide concurrency group, `ci-main`, uses `cancel-in-progress: true`. A newer CI run supersedes an in-progress run; GitHub's concurrency scheduler also replaces a pending run in the same group. The overlapping-run cancellation behavior is configured but has not yet been separately stress-tested with deliberately simultaneous pushes.
- A `cleanup` job runs before the test matrix. It uses the built-in GitHub CLI with `GH_TOKEN: ${{ github.token }}` and requires `actions: write` plus `contents: read`.
- Cleanup is scoped to `.github/workflows/ci.yml`, not every workflow in the repository. It always preserves the current run and the three newest prior CI runs, deleting older completed CI runs. Older runs that are still active are not deleted. This keeps the four newest CI run records while avoiding deletion of active executions.
- The test matrix depends on cleanup, so tests do not start until the retention pass completes.
- The separate `.github/workflows/release.yml` workflow and its run history are intentionally untouched.

### Verified CI results
- Cleanup job: success. It deleted the historical completed CI runs beyond the four-run retention window. The first cleanup pass took longer because it removed the accumulated historical backlog; steady-state runs should normally delete only the one run that falls outside the four-run window.
- Linux job `Test and build (ubuntu-latest)`: formatting passed; `go test -race -cover ./...` passed (81.4% coverage); `go vet ./...` passed; `go build ./...` passed.
- Windows job `Test and build (windows-latest)`: formatting passed; `go test -race -cover ./...` passed (80.9% coverage); `go vet ./...` passed; `go build ./...` passed.
- The Windows race detector worked on the hosted Windows/amd64 runner with Go 1.27.4 and its configured GCC/cgo environment. No extra compiler-install step was needed.
- Both matrix jobs checked out exact HEAD `e34aa2cc60ff6c1fcd994e3e5eaf5851e80602dc`. No local test execution is claimed; all verification evidence is from the GitHub Actions run.

### Post-CI analysis and limitations
- All requested CI behavior is implemented and the complete run is green on both operating systems.
- The cleanup job is deliberately limited to the CI workflow; Release workflow runs remain visible and are not part of the four-run CI retention count.
- Active older runs are retained rather than deleted. The concurrency group should cancel superseded CI runs, but an already-cancelled run may remain active briefly until GitHub finishes cancellation; a later cleanup pass will remove it once completed.
- Cancellation behavior has not been directly observed under concurrent pushes yet. Do not claim a concurrency stress test passed.
- GitHub-hosted Actions emitted non-blocking Node.js 20 deprecation warnings for `actions/checkout@v4` and `actions/setup-go@v5`; the runner forced those actions onto Node.js 24 and the jobs succeeded. Consider action-version maintenance separately rather than mixing it into this CI retention change.

### Next session
1. Confirm `main` HEAD and the newest CI run before any further edits.
2. Keep the main-only trigger policy unless the user changes it.
3. Observe normal future overlapping pushes to confirm that the newest run cancels superseded work. Do not create branches or run an artificial concurrency test without a reason.
4. Preserve the global four-run CI retention policy and keep Release workflow history outside its scope.
5. For any future workflow code change, verify the cleanup job, both OS matrix jobs, exact HEAD SHA, and complete logs; then append a fresh Markdown-only handoff update.


## Session handoff — per-OS coverage reporting and Windows shell correction (2026-11-21)

### Repository and workflow commits
- Repository: `gunesis-game/pulsyflux-stream`; continue directly on `main`.
- Coverage-report workflow commit: `7a071f12bf245cff0c46bb73fa7ed526093f766d` (`ci: publish per-file coverage reports for each OS`).
- Follow-up shell correction commit: `b54bb44bb498a644acfc0f051ce5da21026e4e96` (`ci: run coverage tests with bash on Windows`).
- The workflow change is limited to `.github/workflows/ci.yml`; storage code and tests were not changed.

### What the workflow now does
- Generates `coverage.out` using `go test -race -coverprofile=coverage.out ./...`.
- Prints per-function coverage with `go tool cover -func`, adds the report to the Actions job summary, and uploads the profile and text report as per-OS artifacts retained for seven days.
- The test command, vet, and build steps now explicitly use Bash on both runners. This avoids Windows PowerShell's native argument parsing issue with the coverage-profile command.
- Preserve the existing Linux+Windows matrix, formatting gate, race tests, vet/build, main-only push/manual triggers, global `ci-main` concurrency, and four-run CI retention policy.

### First verification and root cause
- CI run [44899333922](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44899333922), run #161, checked out exact HEAD `7a071f12bf245cff0c46bb73fa7ed526093f766d`.
- Cleanup succeeded. Linux passed formatting, race tests, coverage reporting, artifact upload, vet, and build. Linux aggregate coverage was **81.4%**.
- Windows formatting passed, but the test step failed before producing a profile. PowerShell passed/parsed the command such that Go treated `.out` as a package and reported `no required module provides package .out`. The Windows package test itself reported 80.9%, but the overall command failed due to the extra `.out` package argument. No Windows per-function report or artifact was produced.
- This is a workflow shell/argument parsing defect, not evidence of a source or test failure. Do not use run #161 as a completed cross-platform coverage comparison.

### Follow-up and current verification
- Commit `b54bb44bb498a644acfc0f051ce5da21026e4e96` explicitly sets `shell: bash` for the coverage test, vet, and build steps.
- New push-triggered CI run: [44899535090](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44899535090), run #162, exact head SHA `b54bb44bb498a644acfc0f051ce5da21026e4e96`. At the time this handoff was written, it was still `in_progress`; do not claim it passed until all jobs and logs are inspected.
- The original 0.5 percentage-point coverage discrepancy (81.4% Linux vs 80.9% Windows) remains unexplained. Once run #162 succeeds, compare per-function reports/artifacts. Distinguish intentionally OS-specific code (for example Linux directory-sync support vs Windows fail-closed behavior) from genuinely untested shared logic; do not add tests merely to equalize percentages.
- No local tests or builds are claimed. GitHub Actions is the verification source.
- This handoff update is Markdown-only and should not trigger CI.

### Next steps
1. Verify run #162 to completion against the exact SHA above; inspect cleanup and both OS jobs and their complete logs.
2. Confirm both coverage artifacts uploaded, then compare per-function reports. If needed, inspect the profiles to identify statement-level differences.
3. Only after the reports identify concrete uncovered behavior, add focused tests or document intentional platform exclusions. Preserve the strict Windows `SyncEachAppend` unsupported behavior.
4. Append a new handoff with the verified run #162 outcome and concrete coverage comparison.
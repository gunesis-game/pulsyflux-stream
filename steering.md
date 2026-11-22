# Steering: pulsyflux-stream

## Purpose
Maintain a small, typed, concurrency-safe Go event-stream library. Keep the live-stream core lightweight. Event payload validation/redaction belongs to Event; disk persistence is an internal subsystem.

## Agreed architecture and identity
- **Identity is type-derived, not UUID-derived.** `EventChannel[TStream]` identity is `EventChannel[TStream.name]` (e.g. `EventChannel[AuthStream]`). `Stream[TStream,TEvent]` identity is `Stream[TStream.name,TEvent]` (e.g. `Stream[AuthStream,AuthTokenValidEvent]`). Use a stable canonical type-name representation appropriate to Go types; do not rely on ambiguous bare names if package-qualified identity is needed to avoid collisions.
- `EventChannel` and `Stream` do not own or generate UUIDs. Do not add UUID recovery, identity catalogs, or deterministic UUID derivation.
- Event UUIDs remain for individual event identity unless the user explicitly changes that requirement.
- No schema fingerprint framework. Delete `schema.go` and its fingerprint composition/base/effective machinery. Do not replace it with another schema/catalog subsystem.
- Keep public generic relationship: `EventChannel[TStream]`, `Stream[TStream,TEvent]`, `Event[TEvent]`. One channel family can contain multiple streams with different event types.
- The event/stream/channel human-readable identity strings are type-derived and are used to identify persisted channel/stream records. They are not instance IDs.

## Ownership
- **Event (`event.go`):** typed payload, creation timestamp, redaction policy, payload assignment/validation, serialization/deserialization and event lifecycle. Move any Event-specific functionality currently in `schema.go` into `event.go`; do not leave a schema file behind.
- **Stream (`stream.go`):** concurrency-safe typed delivery of `Event[TEvent]` values to subscribers. Publish sends live unredacted events; persistence redacts before writing.
- **Channel (`channel.go`):** logical channel family based on `TStream`; owns in-memory stream registration and synchronization. No channel UUID.
- **Storage (`storage_disk.go` and related internal files):** append-only disk records, framing/checksum, durability, scanning/recovery and rebuildable metadata. Persist event UUID, channel/stream type-derived identities, sequence, event-creation timestamp, and redacted payload as required by the actual format. Do not persist schema fingerprints or duplicate type identity/timestamp inside payload. Keep storage private; no public storage API without explicit contract/design.
- **Replay:** dispatches historical records through existing typed stream/channel abstractions; replay is not recorded again by default.

## Redaction and payload invariants
- Every Event has a RedactionPolicy; empty policy is explicit opt-out. Policy is copied and immutable.
- Validate exclusion paths at construction and again before payload assignment. Reject blank, duplicate, malformed, or unresolved paths.
- Redact a serialized copy; never mutate live payload. Deserialization cannot recover excluded values.
- Supported traversal: structs, pointers, slices/arrays of structs. Maps and embedded-field flattening are out of scope until deliberately designed and tested.
- No Event envelope. Payload is redacted data directly; event ID, sequence, and timestamp belong to record metadata as appropriate.
- Do not add public Validate/Serialize/Deserialize/TypeName methods or expose event payload/time/type metadata as public fields.

## Stream behavior
- Each stream delivers only `Event[TEvent]` to subscribers. Different `TEvent` streams coexist under one `TStream` channel.
- Duplicate registration of the same event type within one channel returns `ErrDuplicateStream`.
- Publish is atomic across subscribers: if any subscriber is full, return `ErrSlowSubscriber` and deliver to none. Publish with zero subscribers succeeds.
- Preserve concurrency safety for Subscribe, Publish, cancellation, unsubscribe and Close.

## Disk storage principles
- Keep storage taxonomy explicit: lifecycle, write, read/scan, lookup, replay, maintenance, metadata.
- Prefer append-only segmented files. Make framing, checksum, segment rollover, sync policy and recovery boundaries explicit and versioned.
- Flush (application buffering) and Sync (OS/filesystem durability) are distinct; document durability windows.
- Storage handles redacted payload bytes and I/O; it does not own Go payload types or redaction.
- Begin with sequential scan and rebuildable/minimal indexes. No SQL/distributed infrastructure absent concrete requirements.
- Avoid parallel public event/stream/channel/record models unless a real boundary needs them.

## Development rules
- Work directly on `main`; do not create branches unless explicitly requested. Use GitHub MCP for repository operations; cloning is unnecessary.
- Before editing, check current main HEAD, recent commits, relevant files/tests/docs and CI. Do not trust stale handoffs.
- Implement coherent changes, commit directly to main, and review the diff.
- After each code push, identify CI for that exact commit SHA. Wait for completion and inspect conclusion, jobs, steps and logs; never infer success from missing/partial/unrelated runs.
- Disk-storage changes require Linux and Windows CI. Preserve formatting verification, race-enabled tests, vet, build, coverage and artifact upload. Do not weaken checks.
- After completed CI, compare Linux/Windows aggregate statement coverage and per-function coverage from the same run. Explain discrepancies; inspect profiles for statement-level attribution if needed. Do not add artificial tests just to equalize percentages.
- CI formatting must be non-mutating and report offending files.
- Verify formatting, `go test -race -coverprofile=coverage.out ./...`, `go vet ./...`, and `go build ./...` through exact-SHA CI before claiming verified. No local Go execution is claimed unless actually run.
- Do not trigger manually controlled release CI unless explicitly asked.
- After CI analysis, update this file in a separate docs-only commit, including the checkpoint and next task. Markdown-only changes are not CI-tested.
- Keep verified facts separate from assumptions and open decisions. Do not claim unverified work is tested or pushed.

## Handoff hygiene
- This file is the durable source of truth, not a chat transcript. Maintain one concise current handoff in place.
- Once a handoff is resolved, remove it. Move lasting workflow rules here and substantial technical design into canonical `docs/` files; do not preserve history in this file.
- Handoff must record repo/branch, exact HEAD, code SHA and CI run, verification/coverage comparison, changes, risks/open decisions, next task and acceptance criteria, and whether `docs/storage_disk.md` matches implementation.
- If interrupted, state exact stopping point and uncommitted/unpushed state. Never present unverified work as done.

## Current handoff (2026-11-22)
- Repository: `gunesis-game/pulsyflux-stream`, branch `main`.
- Latest code commit: `c9d53f046659809adfd2315c2e67dd2972a76190` (`fix: format v3 disk record fields`). Exact-SHA CI run [44966502951](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44966502951) passed on Ubuntu and Windows: non-mutating formatting, race-enabled tests with coverage, vet, build, and coverage artifacts.
- Coverage in run 44966502951: Linux 86.5%, Windows 86.5% aggregate statements. Per-function coverage was not compared in this checkpoint; percentages match at aggregate level only.
- Docs HEAD is `76633382a27d1a0f2776db6daabe5ca54a5b0c6a`, a docs-only commit that updates `docs/storage_disk.md` for v3. This documentation-only change is not CI-tested. `docs/storage_disk.md` now describes version 3 and its identity fields.
- Code milestone partially landed: `storage_disk.go` uses record version 3 with `ChannelIdentity` and `StreamIdentity` strings, retaining event UUID, sequence, timestamp, payload. `storage_disk_test.go` uses the v3 fields and rejects v2 records. The type identity helpers use package-qualified names for named types and structural `reflect.Type.String()` fallback for unnamed types; generic and unnamed identity tests exist.
- Earlier CI runs failed at formatting only: `b06abd9c` failed due to `storage_disk.go` field alignment; `f877066a` failed for the same alignment. The correction is `c9d53f04`; exact-SHA CI passed.
- **Open work / risk:** this is not a complete persistence/replay integration. The disk writer remains a private, standalone subsystem with no actual typed storage caller/replay path in the current tree. The v3 identity helpers are not yet wired into an end-to-end storage/replay path. Do not describe v3 storage integration or replay preservation as complete.
- Next task: audit the disk backend’s intended integration boundaries and remaining storage/replay requirements against the steering architecture. Confirm whether the repo has a planned/required integration caller; if absent, implement only a coherent, private internal integration with appropriate redaction/serialization ownership, or document an explicit deferral rather than invent a public API. Add tests that prove type identities flow into persisted records and recovery/reader paths as appropriate. Acceptance: v3 identity derivation is exercised through an integration-level path, version 2 is rejected, tests demonstrate record identity round-trip/recovery, docs match code, and Linux/Windows CI is green with per-function and total coverage comparison.
- Current worktree: all changes are committed and pushed; no uncommitted changes are known.

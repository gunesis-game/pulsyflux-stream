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
- Repo: `gunesis-game/pulsyflux-stream`, branch `main`.
- Latest code HEAD: `c9e0d705e4f43a3cc1dec599d785f33591984207` (test file rename/removal). Latest CI for this SHA: [44967119308](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44967119308), queued/pending behind CI cleanup at checkpoint; do not claim it passed until confirmed.
- Most recent full green code CI: commit `21198fb1893719038c8d7f9af1ae243ee92c94ca`, run [44966913051](https://github.com/gunesis-game/pulsyflux-stream/actions/runs/44966913051), Linux and Windows passed format, race tests/coverage, vet, build. Coverage totals: 86.6% each; all 54 per-function coverage report entries matched between platforms.
- Typed persistence adapter `appendEvent[TStream,TEvent]` now validates event/policy, checks set data, serializes and redacts via Event-owned logic, derives channel/stream type identities, preserves creation timestamp, and calls low-level `Append`. Tests cover success round-trip including identities/event ID/sequence/timestamp/redaction and rejection of unset event/invalid policy. This adapter remains private and has no production caller yet; don't claim production storage or replay is wired.
- Code cleanup renames obsolete `schema_test.go` to `event_redaction_test.go` and updates test names. Its exact-SHA CI run 44967119308 is pending; the prior rename commit 44967116599 may be cancelled by the newer run.
- Documentation now describes the adapter, v3 format, and type-derived identity labels: `docs/storage_disk.md`, `docs/type_identity.md`, and README links. Obsolete `docs/schema_identity.md` was deleted. The docs-only changes are not CI-tested; markdown paths-ignore CI.
- Storage v3 fields: channel/stream type identity strings, event UUID, sequence, creation timestamp, redacted payload. V2 and earlier are rejected; no migration/mixed segments. Named type identities are package-qualified; unnamed types use `reflect.Type.String()` fallback, not promised stable across source/toolchain changes.
- Next: complete pending exact-SHA CI for current main. Then review intended lifecycle/storage ownership and recovery/replay architecture; adapter test path is not a real production persistence caller, and no public API is authorized. Identify a coherent internal integration or document explicit deferral; add integration/recovery tests if a concrete private caller is introduced. Acceptance: CI green on current SHA, storage docs match code, no public API added without design, per-platform aggregate and per-function coverage compared.
- Worktree: changes committed/pushed; no uncommitted work known.

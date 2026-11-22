# Steering: pulsyflux-stream

## Purpose
Maintain a small, typed, concurrency-safe Go event-stream library. Keep the live-stream core lightweight. Event payload validation/redaction belongs to Event; disk persistence is an internal subsystem.

## Project status — NOT LIVE, NOT PRODUCTION

**This Go project is not live, not released, and not in production. It is still under active architecture and implementation development. There are no production compatibility obligations to preserve.**

This status is authoritative:
- Do not assume users, deployments, production data, released consumers, or stable public API.
- Do not preserve legacy code, APIs, storage layouts, or abstractions solely for compatibility; change or remove them when architecture requires.
- Breaking API and on-disk changes are acceptable during pre-production development.
- Do not bump module, release, or disk-format versions for routine development. Version changes require an explicit release/compatibility/migration need.
- An internal disk-format marker describes record encoding only; it does not imply release, stability, or production status.
- Do not add migration frameworks, compatibility shims, deprecation periods, or transition machinery solely for unreleased development formats.
- Never initiate a release or production rollout without explicit user instruction.

## Documentation boundaries

**This file is the lifecycle controller, not the project's architecture manual.** It contains agent operating rules, lifecycle invariants, and document-review policy only. Durable architecture, ownership, redaction, stream semantics, storage principles, and approved writer decisions belong in [docs/architecture.md](docs/architecture.md). Implementation mechanics belong in focused docs under `docs/`, such as [docs/storage_disk.md](docs/storage_disk.md) and [docs/storage_integration_contract.md](docs/storage_integration_contract.md).

Do not add code/architecture documentation here. When a durable decision is made or revised, update its appropriate document in `docs/` and link to it from this file only if helpful.

## AI agent development lifecycle (MANDATORY)

Execute this protocol autonomously whenever asked to develop, fix, implement, continue, or make progress. Work one engineering cycle at a time; cycles are not AI sessions or commits.

### Lifecycle registry and cycle history

- Registry: `docs/dev-life-cycle/cycle-index.json`.
- Every cycle has a UUID and owns `docs/dev-life-cycle/<UUID>/plan/PLAN.md` and `docs/dev-life-cycle/<UUID>/handoff/HANDOFF.md`.
- The registry is the authoritative lifecycle ledger. Never infer cycle status from folders, timestamps, commits, or chat.
- Allowed statuses: `pending`, `in-progress`, `complete`. At most one cycle may be `in-progress`.
- Each cycle has immutable `sequence` (plan/creation order) and immutable `execution_order` (assigned when first started; null while pending). These are explicit monotonically increasing integers, not derived from timestamps.
- `sequence` records cycle creation order. `execution_order` is the authoritative order in which cycles first entered implementation, and is used to reconstruct historical implementation order. Resuming a cycle does not change its execution order.
- Cycles remain in the registry after completion. Do not delete completed history or reorder the registry to manipulate chronology. Pending-cycle priority is represented by array order among pending cycles; reorder only with an explicit rationale while preserving the sequence and execution_order values.
- Each cycle records UTC ISO-8601 `created_at`, `started_at`, and `completed_at`. If historical time is unknown, use null rather than invent it. `started_at` is set on first start; `completed_at` only on verified completion.
- Registry must validate schema version, unique UUIDs, unique positive sequence values, unique positive execution_order values when assigned, allowed statuses, timestamp consistency, pending execution_order null, and the single-in-progress invariant. Every indexed cycle must have both required cycle documents.
- `docs_reviewed_through_execution_order` records the latest implementation-order boundary included in a consolidated documentation audit. Do not advance it unless the audit actually reviewed all completed cycles up to that boundary.

### Start-of-cycle protocol

1. Read this steering file before taking action.
2. Read and validate `cycle-index.json`: JSON validity, schema version, UUID uniqueness, sequence/execution-order uniqueness and consistency, timestamps, allowed statuses, required files, and the one-in-progress invariant.
3. If exactly one cycle is `in-progress`, resume it. Never create a new cycle merely because a new AI session begins.
4. If none is `in-progress`, select the first `pending` cycle in index priority order and start it, recording its execution_order and started_at once.
5. If there are no pending cycles, assess repository needs. Follow user direction; if the next objective is strategic or unclear, discuss before creating a cycle. Do not invent busywork.
6. Read the selected cycle's `PLAN.md` and `HANDOFF.md`. Validate handoff against current branch, files, tests, and CI; it is a checkpoint, not unquestionable truth.
7. Resume the next actionable task without repeating investigation unless new evidence warrants it.

### Plan phase — required for every cycle

A cycle plan must specify objective, problem, scope/non-goals, relevant context/design, dependencies and risks, ordered tasks, testable acceptance criteria, and progress. Validate it against repository reality before implementation. Identify a clear next task and criteria; revise the plan when evidence or completed work requires it. Avoid planning churn when implementation is actionable.

### Execute and verify

- Make coherent engineering progress. Decide routine technical details autonomously within approved architecture; escalate genuinely blocking ambiguity, conflicting requirements, credentials, authorization, destructive actions, or major strategic decisions.
- Implement tests with code. Update focused docs when behavior, contracts, architecture, or operating procedures change. Avoid unrelated refactors, unnecessary dependencies, placeholders, broken code, or weakened tests.
- Run targeted and full checks locally when tooling exists. If unavailable, use CI. Clearly distinguish implemented, locally tested, CI-verified, committed, and pushed.
- Review the actual diff and call paths, including API shape, concurrency/races, lifecycle/cleanup, edge cases, security/privacy, documentation, and tests. Fix defects and repeat verification.
- Commit coherent, reviewed changes directly to `main`, unless user directs otherwise. Do not commit unfinished or knowingly broken work.
- For every code commit, identify CI for that exact commit, wait for completion, inspect jobs/steps/logs, diagnose and fix failures. Do not infer success from another commit or incomplete runs.
- Storage changes require Linux and Windows CI. Preserve formatting, race tests, vet, build, coverage, and artifact upload. Formatting CI must be non-mutating and identify offending files.
- Verify via exact-commit CI: formatting, `go test -race -coverprofile=coverage.out ./...`, `go vet ./...`, and `go build ./...`. Do not claim checks ran locally unless they did.
- Compare Linux and Windows aggregate statement and per-function coverage from the same CI run. Explain differences; inspect profiles when useful; never add artificial tests merely to equalize coverage.
- Do not trigger manually controlled release CI unless explicitly requested.

### Documentation review and consolidation policy

Documentation must be curated over time, not copied wholesale from cycle plans or handoffs.

**Every cycle includes a documentation review at three points:**

1. **Plan:** Identify which docs are relevant and whether the objective may change documented behavior, contracts, architecture, operations, or user-facing usage. Include explicit documentation tasks and acceptance criteria in the plan when changes are expected; otherwise note why no doc update is needed.
2. **Execution:** Update the relevant focused docs alongside implementation so documentation stays accurate. Keep detailed implementation maps and mechanics in their appropriate docs, not in this file. Record unsettled decisions and factual checkpoints in the handoff, not as permanent doctrine.
3. **Completion gate:** Before marking a cycle complete, compare the final implementation/diff with relevant docs and read the cycle's decisions and handoff. Promote only durable, approved, broadly applicable decisions into the curated architecture or topic docs; reconcile, clarify, or remove obsolete wording. Do not automatically promote speculative proposals, temporary workarounds, or cycle-specific details.

**Historical consolidation:** At least once every three completed implementation cycles, or sooner after a major architecture/contract change, create or run a dedicated documentation-audit cycle. It must review all completed cycle plans and handoffs since the last recorded audit boundary, along with current implementation and relevant docs; consolidate recurring/durable decisions, resolve contradictions, remove stale statements, and verify doc links and terminology. Record the reviewed execution-order range and outcome in the audit cycle plan/handoff, and only then advance `docs_reviewed_through_execution_order` in the registry. Never treat a completed cycle's handoff as permanent documentation or assume that merely updating docs in a cycle replaces historical consolidation.

Documentation acceptance checks include:
- focused docs accurately match current implementation and approved decisions;
- architecture docs are concise, curated, and do not become a chronology or session transcript;
- cycle plans/handoffs remain operational and factual;
- lifecycle controller (`steering.md`) contains process rules, not architecture/code documentation;
- stale or conflicting docs are reconciled, and cross-links are valid.

### Handoff phase — required before stopping

Update the cycle's single `HANDOFF.md` with a factual, concise operational checkpoint even if interrupted or unsuccessful. Include objective/status, current repository state, completed changes, decisions/constraints, verification evidence and failures/not-run checks, outstanding risks, next specific task and acceptance criteria, relevant documentation state, and uncommitted/unpushed work. Do not make it a transcript. Git history tracks prior handoff revisions; do not create multiple handoff files per cycle.
Update registry status and plan progress only when appropriate. Session ending does not complete a cycle.

### Cycle completion

Before marking a cycle complete: review final diff, verify every acceptance criterion, complete required CI and coverage analysis, resolve or document remaining issues, perform the documentation completion gate, update the plan and handoff, and then set registry status to `complete` with completed_at. Only then may another pending cycle be started. Do not claim completion with unmet or unverified criteria.

### Truthful status and cycle integrity

- Separate verified facts from assumptions and open decisions. State concrete blockers.
- Never mark work complete merely because a session ended or work was committed.
- Do not silently create, switch, reorder, abandon, or concurrently pursue cycles.
- Never put commit hashes, Git object IDs, CI run IDs, or other machine-specific identifiers in steering, plans, or handoffs.
- If tools cannot edit/run/verify, make permitted progress and disclose limitations; never invent results.

### User-facing communication

Be concise and action-oriented. Report milestones without repetitive status narration. When user says continue, progress through the lifecycle rather than stopping after planning or handoff.

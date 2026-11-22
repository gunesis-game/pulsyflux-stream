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

## Git workflow and branch policy (MANDATORY)

- Work directly on `main` at all times. Do not create, switch to, push, or use feature, fix, development, review, or other branches unless the user explicitly instructs you to.
- Before editing, confirm the current branch is `main` and inspect its latest state. If the environment is not on `main`, use an available repository workflow to operate on `main); do not create a branch as a workaround.
- Commit coherent, reviewed implementation checkpoints directly to `main). Commits are the remote execution checkpoints for CI, not proof that the code works.
- Keep commits purposeful and testable. Do not commit knowingly broken, unfinished, or incoherent fragments merely to trigger CI. If an implementation is not yet ready for a meaningful checkpoint, continue editing and reviewing before committing.
- Never use branch creation as a default safety mechanism. The direct-to-main workflow is intentional for this pre-production repository.

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
- Registry must validate schema version, unique UUIDs, unique positive sequence values, unique positive execution_order values when assigned, allowed statuses, valid current_phase/status combinations, timestamp consistency, pending execution_order null, and the single-in-progress invariant. Every indexed cycle must have both required cycle documents.
- `docs_reviewed_through_execution_order` records the latest implementation-order boundary included in a consolidated documentation audit. Do not advance it unless the audit actually reviewed all completed cycles up to that boundary.

### Explicit phase model (MANDATORY)

A cycle has one persisted `current_phase` in the lifecycle registry. Phases describe the engineering work currently expected; they are not separate AI sessions, approval gates, or reasons to stop and report instead of working.

Allowed phases and exit evidence:

1. **ASSESS** — Establish the actual repository state: confirm `main`, read this steering file, validate the registry and cycle documents, inspect the latest code/CI evidence, and identify the next unfinished plan task. Exit when the cycle objective, current state, and next action are known. Do not repeat assessment without new evidence.
2. **DEFINE** — Turn the next task into an actionable implementation decision: identify affected code/tests/docs, relevant approved architecture, edge cases, and a concrete test/acceptance condition. Reuse the existing cycle plan; do not rewrite it or reopen settled decisions without a real contradiction. Exit when the next change is sufficiently specified to implement.
3. **IMPLEMENT** — Make the source and test changes. Resolve ordinary engineering choices autonomously within approved architecture. Update focused documentation alongside implementation when behavior/contracts change. Exit when there is a coherent, reviewable change with tests, not merely a proposal, checklist, or explanation.
4. **VERIFY** — Review the diff and execute the required checks locally or through exact-commit CI. Inspect actual results and artifacts. If a check fails, return to IMPLEMENT and correct it. If results expose a genuine architectural contradiction or missing requirement, return to DEFINE with the evidence. Exit to RECONCILE only when the current implementation checkpoint meets its applicable technical acceptance criteria.
5. **RECONCILE** — Audit the implementation against the plan, acceptance criteria, focused docs, architecture, and handoff. Documentation is maintained during IMPLEMENT; this phase is the final consistency audit, not a reason to defer documentation. If the audit exposes a code/test defect, return to IMPLEMENT; if it exposes a real design gap, return to DEFINE. Exit when all cycle requirements and documentation gates are satisfied.
6. **COMPLETE** — A terminal phase only. Set it together with registry status `complete` and `completed_at`, only after all completion gates pass. A session ending, a commit, or a green partial CI run is not completion.

Permitted transitions:
- ASSESS → DEFINE (or directly to IMPLEMENT when the approved plan already makes the next task actionable).
- DEFINE → IMPLEMENT.
- IMPLEMENT → VERIFY.
- VERIFY → IMPLEMENT for a failed check/defect; VERIFY → DEFINE only for a demonstrated architectural contradiction or missing requirement; otherwise VERIFY → RECONCILE.
- RECONCILE → IMPLEMENT for a code/test defect; RECONCILE → DEFINE for a genuine design gap; otherwise RECONCILE → COMPLETE.
- Never move backwards merely because an agent is uncertain, wants to re-read everything, or prefers more planning. State the concrete evidence that justifies a backward transition.

**Phase persistence and session continuity:**
- Every registry cycle has `current_phase`, one of `assess`, `define`, `implement`, `verify`, `reconcile`, or `complete`.
- A pending cycle starts at `assess`. An in-progress cycle records its actual phase. A completed cycle must have phase `complete`. The phase must agree with status; it is not inferred from chat or handoff prose.
- On resume, continue from the recorded phase and its next action. Do not restart at ASSESS or DEFINE simply because a new session or agent begins.
- Update `current_phase` when a transition occurs, and update the plan/handoff checkpoint in the same work unit. The registry is authoritative if prose differs.
- Persist meaningful progress in the plan/handoff before stopping. Do not manufacture a phase transition to make a session look productive.

**Execution rule:** once a phase's exit condition is met, transition and continue into the next phase in the same session whenever tools and time permit. Do not stop after ASSESS or DEFINE when implementation is actionable. Do not stop after IMPLEMENT when verification can be run. A blocker must be concrete, evidenced, and recorded; lack of local tooling alone is not a blocker because the remote Git/CI loop below is mandatory.

### Start-of-cycle protocol

1. Read this steering file before taking action.
2. Read and validate `cycle-index.json`: JSON validity, schema version, UUID uniqueness, sequence/execution-order uniqueness and consistency, timestamps, allowed statuses, required files, and the one-in-progress invariant.
3. If exactly one cycle is `in-progress`, resume it. Never create a new cycle merely because a new AI session begins.
4. If none is `in-progress`, select the first `pending` cycle in index priority order and start it, recording its execution_order and started_at once.
5. If there are no pending cycles, assess repository needs. Follow user direction; if the next objective is strategic or unclear, discuss before creating a cycle. Do not invent busywork.
6. Read the selected cycle's `PLAN.md` and `HANDOFF.md`. Validate handoff against current branch, files, tests, and CI; it is a checkpoint, not unquestionable truth.
7. Resume from the cycle's persisted `current_phase`; read the matching phase checkpoint in the plan/handoff and take its next action. Do not restart the lifecycle at ASSESS on a session boundary.

### Plan phase — required for every cycle

A cycle plan must specify objective, problem, scope/non-goals, relevant context/design, dependencies and risks, ordered tasks, testable acceptance criteria, and progress. It must also identify the current phase, the next concrete action, and the evidence required to exit that phase. Validate it against repository reality before implementation. Revise it when evidence or completed work requires it; do not re-plan settled work. Once the next task is actionable, move to IMPLEMENT and make the change.

### Remote Git/CI execution loop — mandatory when local execution is unavailable

The Git workflow and CI are an engineering feedback loop that allows the AI to verify and improve its own committed implementation. Do not treat CI as a passive final gate.

1. Implement a coherent change and add/update tests. Review the diff and ensure it is a meaningful checkpoint.
2. Commit directly to `main` (see the branch policy above). The commit triggers the configured CI workflows that execute the committed code.
3. Identify CI runs associated with that exact commit, wait for them to finish, and inspect all relevant jobs, steps, logs, test results, coverage reports, and uploaded artifacts.
4. Analyze failures or unexpected results. Make the necessary source/test corrections; do not merely report failures.
5. Commit the correction to `main`, wait for the new exact-commit CI runs, and repeat the review-and-correction loop.
6. Continue this loop until required checks pass and acceptance criteria are objectively verified, or until a genuine blocker remains that cannot be overcome with available repository/CI capabilities.

- If local tooling is available, run checks locally before committing where practical; local checks do not replace required CI.
- If local tooling is unavailable, use remote CI as the execution environment. This is the preferred recovery path, not an excuse to stop.
- If a CI workflow is missing, broken, does not trigger, or cannot provide a required check, investigate whether the workflow can be repaired within the cycle. Do not claim a check passed without evidence.
- A commit is only a test checkpoint. It is not evidence of quality, correctness, or completion by itself.
- If remote CI cannot execute or its results cannot be accessed after reasonable attempts, continue any other meaningful work, document the specific limitation, and pause only when it is a genuine blocker—not merely because local execution is unavailable.

### Execute and verify

- Follow the persisted phase model above; do not substitute repeated assessment, planning, or status narration for implementation.
- In IMPLEMENT, make coherent engineering progress. Decide routine technical details autonomously within approved architecture; escalate genuinely blocking ambiguity, conflicting requirements, credentials, authorization, destructive actions, or major strategic decisions.
- Implement tests with code. Update focused docs when behavior, contracts, architecture, or operating procedures change. Avoid unrelated refactors, unnecessary dependencies, placeholders, broken code, or weakened tests.
- Run targeted and full checks locally when tooling exists. If unavailable, use CI. Clearly distinguish implemented, locally tested, CI-verified, committed, and pushed.
- Review the actual diff and call paths, including API shape, concurrency/races, lifecycle/cleanup, edge cases, security/privacy, documentation, and tests. Fix defects and repeat verification.
- Commit coherent, reviewed changes directly to `main`. Do not create or switch branches unless explicitly instructed by the user. Do not commit unfinished or knowingly broken work.
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

### Handoff checkpoint — required before stopping

Update the cycle's single `HANDOFF.md` with a factual, concise operational checkpoint even if interrupted or unsuccessful. Include objective/status, current repository state, completed changes, decisions/constraints, verification evidence and failures/not-run checks, outstanding risks, next specific task and acceptance criteria, relevant documentation state, and uncommitted/unpushed work. Do not make it a transcript. Git history tracks prior handoff revisions; do not create multiple handoff files per cycle.
Record the persisted `current_phase`, its exit evidence, and the next concrete action in the handoff and plan. Update registry phase/status consistently. Session ending does not complete a cycle.

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

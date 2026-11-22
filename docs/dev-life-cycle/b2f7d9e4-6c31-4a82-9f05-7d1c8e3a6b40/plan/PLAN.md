# Cycle Plan

## Objective
Consolidate and audit project documentation after the first major async-persistence/ErrorStream architecture change. Reconcile the public README and all current architecture/storage/lifecycle documents with the implementation and the completed cycle's plan and handoff.

## Problem
The previous implementation cycle completed channel-owned ErrorStream routing and removed the stream lookup registry and `Get()`, but the README still says those changes are incomplete. The lifecycle registry also records that no historical documentation consolidation has yet covered execution order 1. This is a documentation correctness issue, not a request for new runtime functionality.

## Scope
- Audit every completed cycle plan and handoff since `docs_reviewed_through_execution_order` (currently 0), beginning with execution order 1.
- Compare the completed cycle's durable decisions and acceptance criteria with current code and the curated architecture, storage, type-identity, and integration-contract docs.
- Correct README API/lifecycle/persistence statements, including the ErrorStream and removed-registry behavior.
- Reconcile stale or contradictory wording found in the audited docs; do not duplicate implementation details in the README or steering controller.
- Validate relative Markdown links and terminology across all maintained Markdown docs.
- Record the reviewed execution-order range and audit outcome; advance the registry audit boundary only when the entire audit is complete.

## Non-goals
- No runtime code changes or new public API.
- No replay implementation, durability expansion, storage migration, release, or version bump.
- No speculative architecture changes or unrelated prose rewrites.

## Context and constraints
- Project remains pre-production; breaking changes are acceptable and no release is authorized.
- `steering.md` is the lifecycle controller, not the architecture manual.
- Durable architecture belongs in `docs/architecture.md`; storage mechanics/contracts belong in focused storage docs; user-facing usage belongs in `README.md`.
- Cycle 1 is the only completed implementation cycle and is the full historical range to audit (execution order 1).
- Current implementation includes `EventChannel.Errors(bufferSize)`, one lazy channel-owned system-only ErrorStream, terminal-cause routing, reserved notification capacity, writer-owner fan-out, and no stream registry/`Get()`.
- Preserve the existing main-only workflow and exact-commit Linux/Windows CI requirements.

## Dependencies and risks
- Depends on the completed cycle's plan/handoff, current source, and current docs.
- Main risk is accidentally turning historical handoff detail into permanent architecture or weakening documented durability limits. Promote only decisions already approved and implemented.
- README claims must remain consistent with source signatures and current pre-production status.

## Ordered tasks
1. Read the complete completed-cycle plan and handoff and validate their claims against current implementation and CI evidence.
2. Audit `README.md`, `docs/architecture.md`, `docs/storage_disk.md`, `docs/storage_integration_contract.md`, `docs/type_identity.md`, and `steering.md` for stale statements, duplicated doctrine, ownership/API mismatches, and terminology.
3. Update README to describe the current public channel/error-stream behavior and remove obsolete claims that integration/removal is incomplete.
4. Reconcile any other evidenced documentation contradiction; retain explicit non-goals and unsupported guarantees such as deferred replay and unsupported strict directory-entry durability.
5. Validate all relative Markdown links and check terminology against current source.
6. Run exact-commit Linux and Windows CI, inspect jobs/steps/logs and artifacts, and repair any failures (documentation-only changes still require the configured CI loop).
7. Complete the audit record in this plan and handoff, then advance `docs_reviewed_through_execution_order` to 1 and mark the cycle complete only after all gates pass.

## Acceptance criteria
- All completed cycles since the prior audit boundary (execution order 1 only) have their plan and handoff reviewed.
- README no longer claims ErrorStream routing or registry/`Get()` removal is incomplete.
- README accurately explains `Errors(bufferSize)`, normal subscription, system-only failure notification, and current persistence/queue-admission limitations without exposing private storage APIs.
- Curated architecture and focused docs agree with implementation; no stale or contradictory status statement remains in maintained docs.
- Relative Markdown links resolve; terminology and public API names match source.
- No unapproved runtime, release, format-version, or architecture changes are introduced.
- Exact-commit Linux and Windows CI pass all configured formatting, race-test/coverage, vet, build, and artifact-upload steps.
- The registry audit boundary advances to execution order 1 only after the audit is complete; the cycle is marked complete with a UTC completion timestamp.

## Persisted execution phase
- **Current phase:** IMPLEMENT
- **Why:** ASSESS and DEFINE are complete. The audit scope is bounded to one completed cycle, the README contradiction is confirmed against source and focused docs, and the corrective documentation change is directly actionable.
- **Phase exit:** README and any evidenced doc corrections are coherent and reviewed; then transition to VERIFY.
- **Next concrete action:** Update README's channel lifecycle and private persistence sections, removing the obsolete bottom status paragraph.

## Progress
- [x] ASSESS — confirmed main, read steering, validated registry invariants, inspected current docs/source/CI checkpoint, and identified the stale README claims.
- [x] DEFINE — bounded the audit to execution order 1 and specified README corrections, link checks, and exact-commit CI acceptance.
- [ ] IMPLEMENT — reconcile README and any other confirmed doc contradictions.
- [ ] VERIFY — inspect exact-commit Linux and Windows CI and artifacts.
- [ ] RECONCILE — compare all audited docs and cycle decisions; record reviewed range.
- [ ] COMPLETE — advance documentation audit boundary to 1 and complete registry/plan/handoff together.

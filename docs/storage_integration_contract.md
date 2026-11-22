# Internal Persistence Integration Contract (Draft)

> Status: design draft only. This document proposes semantics for connecting the private disk adapter to live streams. It does not authorize a public API or production wiring.

## Goals

- Preserve current typed live-stream semantics, including all-or-nothing subscriber delivery when any subscriber buffer is full.
- Make a persist-enabled publish observable and predictable on failure.
- Keep event redaction and serialization owned by `Event`; storage owns only records and I/O.
- Keep replay separate from normal Publish so historical records are not appended again.
- Avoid an unbounded queue or silent loss.

## Proposed synchronous contract

For a stream configured with a private persistence sink, each publish is serialized by the stream's existing mutex:

1. Reject closed stream, invalid event, or unset payload.
2. Preflight all subscriber buffers. If any is full, return `ErrSlowSubscriber` and do not persist or deliver.
3. Prepare redacted payload and append one record to the sink.
4. If persistence fails, return the storage error and deliver to no subscriber.
5. If append succeeds, deliver the original unredacted Event to every subscriber and return nil.

This is a **persist-before-live-delivery** proposal. It aligns accepted persisted sequence order with live delivery order and avoids live-only delivery when the sink rejects the event. Because the disk writer is buffered by default, successful Append means accepted into the writer's userspace buffer—not crash durability. Callers needing filesystem durability would have to request Sync separately; strict per-append directory durability remains unsupported.

## Failure and backpressure semantics

| Condition | Proposed behavior |
|---|---|
| Stream/channel closed | Return `ErrClosed`; no append or delivery |
| Event invalid or data unset | Return validation error; no append or delivery |
| Subscriber buffer full | `ErrSlowSubscriber`; no append or delivery |
| Sink append fails | Return sink error; no live delivery |
| No subscribers | Persist (if configured) and succeed |
| No persistence sink | Keep current live-only Publish behavior |

No asynchronous queue is proposed. A synchronous sink can delay Publish while I/O/rotation runs; this is explicit backpressure. If asynchronous persistence is desired later, it requires a bounded queue size, queue-full policy, ordering guarantee, shutdown drain behavior, and error reporting channel before implementation.

## Ownership/lifecycle proposal

- The persistence sink is attached to an individual typed Stream, not the EventChannel. This avoids ambiguity when one channel family owns multiple event types and their different payload/redaction rules.
- The sink is injected internally at stream construction; the public `New` signature stays unchanged until a separate API design is approved.
- Stream.Close does not implicitly close a shared writer unless exclusive ownership is explicit. Writer lifetime is managed by a private storage owner; Close/Flush/Sync errors must be surfaced, not discarded.
- Avoid multiple writers for the same directory. Multi-process locking is out of scope; storage remains single-process and single-writer.

## Replay proposal

Replay should use a separate internal delivery path that does not call Publish and therefore does not append again. It should decode a record using the registered type, verify channel/stream identities, and deliver to subscribers with defined behavior for backpressure. Unknown identities, payload decode failures, and corrupt records should stop replay rather than skip silently. Replay ordering uses persisted sequence, not timestamp.

**Recommended initial replay scope:** replay is startup-only, before live Publish is enabled for a persist-enabled stream. Do not allow replay and live publish to interleave. A replay operation may be bounded by subscriber capacity and return `ErrSlowSubscriber`; it must not advance its cursor/sequence on an event that was not delivered. If replay is resumed, the cursor must identify the last successfully delivered persisted sequence, not merely the last record read. For a first implementation, avoid persistent checkpoints: the caller may replay from the beginning, and replay is only offered when subscriber capacity can be managed without blocking indefinitely.

The remaining product/API decisions are whether replay should be exposed at all in the initial storage integration, and whether the startup-only replay contract should use an explicit internal lifecycle state or be guaranteed by construction-time wiring.

## Recommended contract decisions

These are the design recommendations to implement only after approval of storage integration:

- **Ordering:** synchronous persist-before-live-delivery under the stream mutex; append sequence and subscriber-observed live order agree.
- **Failure atomicity:** no live delivery if validation, slow-subscriber preflight, or sink append fails. Storage errors propagate directly from Publish.
- **Backpressure:** synchronous I/O; no asynchronous queue. Slow subscribers reject before append, so there is no orphan persisted event.
- **Ownership:** private writer owner injected per stream; stream close does not close shared storage unless explicit exclusive ownership is defined. No multi-process lock.
- **Replay:** separate startup-only path; never invokes Publish or append; deliver by stored sequence; stop on unknown type, identity mismatch, corrupt frame, or payload decode error. The initial integration may omit replay until lifecycle and buffer strategy are fully specified.
- **Durability:** append success means buffered writer acceptance only. Explicit Sync is separate; strict directory-entry durability is unsupported on both CI platforms.

## Decision checklist before wiring

- [ ] Confirm the recommended persist-before-deliver ordering and append failure behavior.
- [ ] Confirm synchronous backpressure and no queue.
- [ ] Define private writer owner, sharing, flush/sync, and shutdown behavior.
- [ ] Decide whether replay is included in the first integration or deferred.
- [ ] Add tests for ordering under concurrent publishers, slow subscribers (no orphan persisted record), storage failure (no live delivery), redaction, no subscribers, Close, and replay non-reappend.

## Non-goals

No public storage configuration/API, automatic recovery migration, cross-process writer locking, unbounded asynchronous queue, or silent partial persistence/delivery is proposed here.

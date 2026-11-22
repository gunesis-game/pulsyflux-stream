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

The draft leaves open whether replay is allowed only before live publishing begins, how replay conflicts with subscriber buffer capacity are handled (batching/waiting vs returning `ErrSlowSubscriber`), and whether a replay cursor or checkpoint is required.

## Decision checklist before wiring

- [ ] Confirm persist-before-deliver ordering, or choose a different explicit ordering.
- [ ] Confirm sink append failure blocks live delivery.
- [ ] Confirm synchronous I/O backpressure and no queue.
- [ ] Define private writer owner, sharing, flush/sync, and shutdown behavior.
- [ ] Define replay startup phase and subscriber backpressure strategy.
- [ ] Add tests for ordering under concurrent publishers, slow subscribers (no orphan persisted record), storage failure (no live delivery), redaction, no subscribers, Close, and replay non-reappend.

## Non-goals

No public storage configuration/API, automatic recovery migration, cross-process writer locking, unbounded asynchronous queue, or silent partial persistence/delivery is proposed here.

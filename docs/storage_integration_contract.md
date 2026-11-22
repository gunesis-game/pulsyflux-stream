# Internal Persistence Integration Contract (Draft)

> Status: architecture decisions approved; implementation not yet integrated. This document is the contract for the first private async persistence integration. Replay is deliberately deferred.

## Goals

- Preserve current typed live-stream semantics, including all-or-nothing subscriber delivery when any subscriber buffer is full.
- Make a persist-enabled publish observable and predictable on failure.
- Keep event redaction and serialization owned by `Event`; storage owns only records and I/O.
- Keep replay separate from normal Publish so historical records are not appended again.
- Avoid an unbounded queue or silent loss.

## Agreed asynchronous batching contract

Persistence uses a private asynchronous writer with a bounded in-memory queue and small append batches. Individual Publish calls must not perform disk writes.

1. Publish validates the event and preflights all subscriber buffers using the existing all-or-nothing live-delivery rule.
2. The library prepares the redacted event record and submits it to the bounded writer queue.
3. Queue admission is the persistence handoff point. Publish does not wait for the disk append; after successful admission it delivers the original unredacted Event to subscribers and returns.
4. The writer collects multiple queued records and appends them as a batch. It flushes when a batch-size threshold is reached or a short maximum batching delay expires. Exact internal thresholds remain an implementation choice; they are not public configuration.
5. The queue is bounded. When it is full, Publish applies backpressure by waiting for queue capacity rather than growing memory without limit.
6. Queue admission does not mean the record has reached disk. A process crash may lose queued records or records in an unflushed batch.

This changes the durability meaning of Publish: successful Publish means the event was accepted by the library's bounded persistence queue and delivered live, not that it was persisted or crash-durable. Flush and Sync remain distinct operations.

## Failure and backpressure semantics

| Condition | Agreed/proposed behavior |
|---|---|
| Stream/channel closed | Return `ErrClosed`; no queue admission or delivery |
| Event invalid or data unset | Return validation error; no queue admission or delivery |
| Subscriber buffer full | `ErrSlowSubscriber`; no queue admission or delivery |
| Writer queue full | Apply bounded backpressure; wait for queue capacity |
| Queue admission succeeds | Deliver live event; disk append may happen later |
| Asynchronous disk write fails | Writer enters terminal failed state; notify each distinct channel represented by the failed batch and queued records, once per failure; preserve the first cause |
| No subscribers | Persist-enabled Publish still queues the event |
| No persistence enabled | Keep live-only Publish behavior |

The writer must preserve accepted record order. A disk failure can occur after Publish has returned and live delivery has happened, so it cannot be represented solely as a Publish return error and cannot undo live delivery. On a fatal write failure, the shared writer enters a terminal failed state and preserves the first underlying cause. It reports the failure through the private ErrorStream path for each distinct EventChannel owner represented in the failed batch and the remaining queued records; owners are deduplicated for the failure. Each queued record carries a private, non-persisted owner reference solely for this routing. This does not create a global channel registry. Queued records that cannot be written are not silently discarded as successful persistence; they are rejected/drained as failed work and subsequent persisted Publish calls return an error wrapping the terminal cause.

## Ownership/lifecycle proposal

- Disk storage is created/opened and owned entirely inside the library. It is never passed in, returned, configured, or otherwise exposed through the public API. No public store type, interface, constructor, option, parameter, or handle is permitted.
- **Event records are the only persisted record type.** There are no persisted channel records, stream records, channel catalogs, manifests, or synthetic records. Each event record carries its type-derived channel and stream identities, event UUID, sequence, creation timestamp, and redacted payload.
- A channel or stream with no events has no persisted representation. Channel/stream construction must not load or create a separate persisted entity. Existing events are located by scanning/filtering event records using the type-derived identities already stored on each record.
- The channel/stream layer uses private storage operations internally. The private storage root is `<current-working-directory>/pulsyflux-stream-data`, resolved to an absolute path when internal storage is initialized. The library creates the directory as needed and owns its lifecycle; callers do not provision, configure, or receive the path. Writer sharing is process-wide and privately managed with active-owner counting only; no channel registry is maintained. Each queue item carries a private EventChannel owner reference for error fan-out. Individual Close releases ownership; final-owner Close stops admission, drains accepted work, flushes and closes. Channel error streams remain open through failure routing, then close. Do not introduce public dependency injection.
- Use one writer per process for the private storage directory. Multi-process locking is out of scope. The writer manager maintains only an active-owner count, not a channel registry. Queue records carry private EventChannel owner references only to route terminal failures; this queue-local ownership is not a registry. On failure, deduplicate owner references across the failed batch and pending queue and notify each once. A channel that subsequently publishes against the terminal writer receives an error and its own internal error notification.

## Replay (deferred)

Replay should use a separate internal delivery path that does not call Publish and therefore does not append again. It should decode a record using the registered type, verify channel/stream identities, and deliver to subscribers with defined behavior for backpressure. Unknown identities, payload decode failures, and corrupt records should stop replay rather than skip silently. Replay ordering uses persisted sequence, not timestamp.

**Future replay scope (not part of initial persistence integration):** replay is startup-only, before live Publish is enabled for a persist-enabled stream. Do not allow replay and live publish to interleave. A replay operation may be bounded by subscriber capacity and return `ErrSlowSubscriber`; it must not advance its cursor/sequence on an event that was not delivered. If replay is resumed, the cursor must identify the last successfully delivered persisted sequence, not merely the last record read. For a first implementation, avoid persistent checkpoints: the caller may replay from the beginning, and replay is only offered when subscriber capacity can be managed without blocking indefinitely.

Replay is explicitly outside the first persistence integration. Do not implement or test replay as part of the initial writer work; remove replay from the first integration acceptance checklist. When implemented later, startup-only replay must not interleave with live Publish.

## Current contract decisions and remaining proposals

- **Ordering:** queue admission must preserve Publish acceptance order. Live delivery follows successful queue admission; disk append occurs asynchronously in accepted order.
- **Failure behavior:** validation and slow-subscriber preflight failures happen before queue admission and live delivery. A fatal asynchronous disk failure may occur after Publish returned and live delivery happened; the writer becomes terminal and reports the first failure to each distinct channel represented in the failed batch or pending queue through that channel's internal ErrorStream delivery path. Each queue item carries a private owner reference; no global channel registry is introduced. Future persisted Publish calls return an error wrapping the terminal cause and emit the channel-local error notification.
- **Backpressure:** bounded queue; when full, Publish waits for capacity. Batch writes are triggered by a size threshold or short maximum delay. Exact thresholds are private implementation details.
- **Ownership:** all storage remains private. One process-wide writer is shared by active EventChannels; the shared manager tracks only active ownership count, not a registry of channels. Each channel Close marks that channel closed and releases its writer ownership; the writer remains alive while other channels are active. The final owner Close stops queue admission, drains accepted queue work, flushes buffered writes, closes the writer, and returns the resulting shutdown error. The writer does not call Sync unless a separately supported durability operation requests it; strict directory-entry durability remains unsupported. Channel error streams must remain open until writer shutdown/error notification routing completes, then close with the channel's other streams. No multi-process lock.
- **Replay:** deferred entirely. It must not block the first persistence integration.
- **Durability:** successful Publish means queue acceptance, not disk persistence. A crash may lose queued or unflushed records. Flush and Sync are separate; strict directory-entry durability is unsupported on both CI platforms.

## Decision checklist before wiring

- [x] Agree asynchronous bounded batching: Publish queues before live delivery; a private writer batches records; queue-full applies backpressure; successful Publish is not a durability guarantee.
- [x] Define async write-error reporting: terminal writer failure, first cause retained, queued-owner error fan-out, and subsequent persisted publishes fail.
- [x] Define private writer sharing and shutdown: active-owner count, per-channel release, final-owner drain/flush/close, and keep channel ErrorStreams open through failure notification. Storage remains private and event-record-only.
- [x] Defer replay from initial persistence integration; reader filtering/routing and replay lifecycle will be handled as a separate task.
- [ ] Add tests for ordering under concurrent publishers, slow subscribers (no orphan persisted record), storage failure (no live delivery), redaction, no subscribers, queue backpressure, terminal writer error fan-out, per-channel Close while other owners remain, final-owner drain/flush/close and Close idempotency. Replay tests are deferred.

## Non-goals

No public storage configuration/API, automatic recovery migration, cross-process writer locking, unbounded asynchronous queue, or silent partial persistence/delivery is proposed here.

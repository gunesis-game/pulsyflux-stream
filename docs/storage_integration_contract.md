# Internal Persistence Integration Contract (Draft)

> Status: design draft only. This document proposes semantics for connecting the private disk adapter to live streams. It does not authorize a public API or production wiring.

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
| Asynchronous disk write fails | Report through a library error mechanism; exact mechanism and subsequent-writer behavior remain undecided |
| No subscribers | Persist-enabled Publish still queues the event |
| No persistence enabled | Keep live-only Publish behavior |

The writer must preserve accepted record order. A disk failure can occur after Publish has returned and live delivery has happened, so it cannot be represented solely as a Publish return error and cannot undo live delivery. The library's asynchronous error-reporting mechanism, whether the writer stops accepting new records after a fatal error, and how queued records are handled on failure remain open decisions.

## Ownership/lifecycle proposal

- Disk storage is created/opened and owned entirely inside the library. It is never passed in, returned, configured, or otherwise exposed through the public API. No public store type, interface, constructor, option, parameter, or handle is permitted.
- **Event records are the only persisted record type.** There are no persisted channel records, stream records, channel catalogs, manifests, or synthetic records. Each event record carries its type-derived channel and stream identities, event UUID, sequence, creation timestamp, and redacted payload.
- A channel or stream with no events has no persisted representation. Channel/stream construction must not load or create a separate persisted entity. Existing events are located by scanning/filtering event records using the type-derived identities already stored on each record.
- The channel/stream layer uses private storage operations internally. The private storage root is `<current-working-directory>/pulsyflux-stream-data`, resolved to an absolute path when internal storage is initialized. The library creates the directory as needed and owns its lifecycle; callers do not provision, configure, or receive the path. Private writer-sharing and shutdown ownership must still be settled; do not introduce public dependency injection to solve them.
- Avoid multiple writers for the same directory. Multi-process locking is out of scope; storage remains single-process and single-writer. The asynchronous writer uses a bounded queue and batches records; its owner/sharing scope and shutdown drain/flush behavior remain open.

## Replay proposal

Replay should use a separate internal delivery path that does not call Publish and therefore does not append again. It should decode a record using the registered type, verify channel/stream identities, and deliver to subscribers with defined behavior for backpressure. Unknown identities, payload decode failures, and corrupt records should stop replay rather than skip silently. Replay ordering uses persisted sequence, not timestamp.

**Recommended initial replay scope:** replay is startup-only, before live Publish is enabled for a persist-enabled stream. Do not allow replay and live publish to interleave. A replay operation may be bounded by subscriber capacity and return `ErrSlowSubscriber`; it must not advance its cursor/sequence on an event that was not delivered. If replay is resumed, the cursor must identify the last successfully delivered persisted sequence, not merely the last record read. For a first implementation, avoid persistent checkpoints: the caller may replay from the beginning, and replay is only offered when subscriber capacity can be managed without blocking indefinitely.

The remaining product/API decisions are whether replay should be exposed at all in the initial storage integration, and whether the startup-only replay contract should use an explicit internal lifecycle state or be guaranteed by construction-time wiring.

## Current contract decisions and remaining proposals

- **Ordering:** queue admission must preserve Publish acceptance order. Live delivery follows successful queue admission; disk append occurs asynchronously in accepted order.
- **Failure behavior:** validation and slow-subscriber preflight failures happen before queue admission and live delivery. An asynchronous disk failure may occur after Publish returned and live delivery happened; it must be reported through a library error mechanism that is not yet designed.
- **Backpressure:** bounded queue; when full, Publish waits for capacity. Batch writes are triggered by a size threshold or short maximum delay. Exact thresholds are private implementation details.
- **Ownership:** all storage remains private. Writer owner/sharing scope, close behavior, queue drain, and final flush/sync behavior remain undecided. No multi-process lock.
- **Replay:** separate startup-only path; never invokes Publish or append; deliver by stored sequence; stop on unknown type, identity mismatch, corrupt frame, or payload decode error. The initial integration may omit replay until lifecycle and buffer strategy are fully specified.
- **Durability:** successful Publish means queue acceptance, not disk persistence. A crash may lose queued or unflushed records. Flush and Sync are separate; strict directory-entry durability is unsupported on both CI platforms.

## Decision checklist before wiring

- [x] Agree asynchronous bounded batching: Publish queues before live delivery; a private writer batches records; queue-full applies backpressure; successful Publish is not a durability guarantee.
- [ ] Define asynchronous write-error reporting and writer behavior after a disk failure.
- [ ] Define private writer ownership/sharing, flush/sync, shutdown drain behavior, and the library's private storage-root policy. The design must operate on event records only; no channel/stream entity records or catalogs. Never expose the store or its location/configuration as public API.
- [ ] Define how the reader filters/routes event records by embedded channel/stream identity, and decide whether replay is included in the first integration or deferred.
- [ ] Add tests for ordering under concurrent publishers, slow subscribers (no orphan persisted record), storage failure (no live delivery), redaction, no subscribers, Close, and replay non-reappend.

## Non-goals

No public storage configuration/API, automatic recovery migration, cross-process writer locking, unbounded asynchronous queue, or silent partial persistence/delivery is proposed here.

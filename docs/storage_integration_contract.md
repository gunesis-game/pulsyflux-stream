# Internal Persistence Integration Contract

> **Current status:** private asynchronous persistence is integrated into live `Publish`. Redacted events are admitted to a bounded queue and delivered live after admission. ErrorStream terminal-failure routing, queue saturation/backpressure stress, and removal of `Get()`/the stream registry remain incomplete. Replay is deferred.

## Public and ownership boundaries

- Disk storage remains entirely private: no public store type, constructor, interface, option, configuration, handle, or injection point.
- `NewEventChannel[TStream]()` owns private storage lifecycle and returns constructor errors explicitly.
- Event records are the only persisted record type. There are no channel/stream records, catalogs, or manifests.
- EventChannel identity and Stream identity are derived from canonical Go type identities and stored as record metadata; they are not instance IDs.
- The private storage root is `<cwd>/pulsyflux-stream-data`, resolved to an absolute path when initialized.
- A process-wide writer is shared by active EventChannels. Its manager tracks active ownership count only, not a channel registry. Queue entries hold private owner references solely for error fan-out.
- Event owns payload validation, redaction and serialization. Stream owns typed live delivery. Channel owns typed identity and private storage lifecycle.

## Publish and async batching

1. Validate the event and preflight all subscriber buffers, preserving all-or-nothing delivery.
2. Prepare the redacted payload and submit it to the bounded writer queue.
3. After successful admission, deliver the original unredacted event live and return.
4. The writer appends records in accepted order, batching up to 32 records or a maximum delay of 25 ms.
5. Queue capacity is 256. When full, Publish waits for capacity rather than allowing unbounded memory growth.
6. Persistence occurs even when there are no subscribers. A slow-subscriber rejection occurs before admission and is not persisted.

Successful Publish means queue admission and live delivery, not disk persistence or crash durability. Queued or unflushed records can be lost on process crash. Flush and Sync are distinct; strict directory-entry durability and multi-process locking are not supported.

## Failure handling

- Validation, unset-data, closed-channel, and slow-subscriber failures happen before queue admission and live delivery.
- Fatal asynchronous disk failure makes the shared writer terminal and preserves the first underlying cause.
- Pending and failed-batch queue owners are deduplicated and notified once per failure through their channel-local internal ErrorStream. This routing must not use a global channel registry.
- Future persisted Publish attempts return an error wrapping the retained cause and notify the owning channel's ErrorStream.
- System-error events bypass persistence to avoid recursive failures and retain the cause for `errors.Is`/`errors.As`.
- ErrorStream is channel-owned, one per EventChannel, and exposed only through `Errors(bufferSize)`, which returns a typed `*Stream[ErrorStream, StreamWriteErrorEvent]`. Consumers use ordinary `Subscribe(ctx)`. Reserved system-error delivery capacity prevents business-event buffers from consuming notification capacity.
- ErrorStream stays open until channel writer ownership is released and failure notifications are routed, then closes with the channel's other streams.

## Shared writer lifecycle

Each EventChannel acquires shared writer ownership at construction. Closing one channel releases only its ownership; the writer remains active while other owners exist. Final-owner Close stops queue admission, drains accepted work, flushes and closes storage, returning shutdown errors. Close is idempotent. A later channel can initialize a fresh writer.

## Replay (deferred)

Replay is not part of the initial persistence integration. When implemented, it must use a separate typed delivery path (never Publish), preserve record sequence order, verify identities, and avoid interleaving with live Publish. Corruption, unknown identities, or decode errors must not be silently skipped.

## Current implementation checkpoint

**Verified code:** private bounded async writer, shared writer ownership manager, redacted Publish persistence, persistence with zero subscribers, slow-subscriber preflight, and concurrent producer persistence test. Verified code commit: `ef0af7645e366591e5f3b50d7371055500459c9a`; CI run `45004383109` passed Linux and Windows for formatting, race tests/coverage, vet, build, and coverage artifact upload. Both platforms reported 82.4% aggregate statement coverage; per-file/function coverage matched.

**Not yet implemented/verified:** ErrorStream creation and terminal failure routing; removal of `Get()` and map-based stream registry; queue saturation/backpressure stress; per-channel close while other owners remain; final-owner shutdown stress; injected storage failures.

## Acceptance criteria for the next code milestone

- Implement channel-owned ErrorStream and typed `Errors(bufferSize)` access boundary.
- Route terminal writer failures to affected channel owners once; bypass persistence and preserve causes.
- Test injected terminal failure, owner deduplication, reserved error capacity, and subsequent Publish behavior.
- Remove `Get()` and the stream registry in line with steering; retain correct Close/subscriber lifecycle without adding a replacement lookup registry.
- Add queue saturation/backpressure and writer/channel lifecycle stress coverage.
- Run exact-SHA Linux and Windows CI. Compare aggregate and per-function coverage from the same run; do not claim verification without it.

## Non-goals

No public storage configuration, replay implementation, migration framework, cross-process locking, unbounded queue, release, or version bump.

# Internal Persistence Integration Contract

> **Current status:** private asynchronous persistence is integrated into live `Publish`. Channel-owned terminal ErrorStream routing is implemented, and the retrieval-only stream registry/`Get()` API has been removed. Focused verification and remaining stress/failure-injection coverage are in progress. Replay remains deferred.

## Public and ownership boundaries

- Disk storage remains entirely private: no public store type, constructor, interface, option, configuration, handle, or injection point.
- `NewEventChannel[TStream]()` owns private storage lifecycle and returns constructor errors explicitly.
- Event records are the only persisted record type. There are no channel/stream records, catalogs, or manifests.
- EventChannel identity and Stream identity are derived from canonical Go type identities and stored as record metadata; they are not instance IDs.
- The private storage root is `<cwd>/pulsyflux-stream-data`, resolved to an absolute path when initialized.
- A process-wide writer is shared by active EventChannels. Its manager tracks active ownership count only, not a channel registry. Queue entries hold private owner references solely for error fan-out.
- Event owns payload validation, redaction and serialization. Stream owns typed live delivery. Channel owns typed identity and private storage lifecycle.
- EventChannel does not retain a stream lookup registry. Callers retain the stream values returned by `New`; the retrieval-only `Get()` API is removed.

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
- Pending and failed-batch queue owners are deduplicated and notified once per channel through the channel-owned internal ErrorStream. This routing does not use a global channel registry.
- `EventChannel.Errors(bufferSize)` lazily returns the one typed `*Stream[ErrorStream, StreamWriteErrorEvent]` for that channel. Consumers use ordinary `Subscribe(ctx)`.
- The error event retains the original cause in its `Cause` field, so `errors.Is` and `errors.As` remain available. Error notifications bypass persistence to avoid recursive failures.
- One subscriber slot is reserved for the terminal system notification. The system stream rejects public Publish calls.
- A terminal cause is retained even if `Errors` has not yet been requested; requesting it later delivers the retained notification.
- ErrorStream remains open through writer ownership release and terminal failure routing, then closes with the channel.

## Shared writer lifecycle

Each EventChannel acquires shared writer ownership at construction. Closing one channel releases only its ownership; the writer remains active while other owners exist. Final-owner Close stops queue admission, drains accepted work, flushes and closes storage, returning shutdown errors. Close is idempotent. A later channel can initialize a fresh writer.

Normal stream subscriptions observe the EventChannel close signal and unregister themselves; no channel-wide stream lookup/close registry is maintained.

## Replay (deferred)

Replay is not part of the initial persistence integration. When implemented, it must use a separate typed delivery path (never Publish), preserve record sequence order, verify identities, and avoid interleaving with live Publish. Corruption, unknown identities, or decode errors must not be silently skipped.

## Verification still required

- Inject append and flush failures and prove first-cause preservation, owner fan-out/deduplication, queue drain, and blocked-producer wake-up.
- Prove ErrorStream cause propagation, one-notification semantics, reserved capacity, lazy creation after failure, and close ordering.
- Stress bounded-queue saturation/backpressure, concurrent producers, channel Close while other owners remain, and final-owner shutdown.
- Run exact-commit Linux and Windows CI for formatting, race-enabled tests/coverage, vet, build, and artifact upload.
- Compare aggregate statement and per-function coverage from the same CI run; inspect platform differences rather than attempting to equalize coverage.

## Non-goals

No public storage configuration, replay implementation, migration framework, cross-process locking, unbounded queue, release, or version bump.

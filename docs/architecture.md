# Architecture and Engineering Decisions

This document is the curated home for durable architecture, identity, ownership, payload/redaction, stream, disk-storage, and writer-lifecycle decisions. It records the agreed direction; implementation-specific details and current checkpoints belong in focused documents such as [Disk Storage Architecture](storage_disk.md), and may be superseded by implementation evidence.

## Project maturity and compatibility

The project is not live, released, or in production. There are no production compatibility obligations. Breaking API and on-disk changes are acceptable while pre-production. Prefer the correct architecture over legacy compatibility; do not add migration frameworks, shims, or compatibility machinery without a concrete requirement. Do not bump module, release, or disk-format versions for routine development. Never release or roll out without explicit instruction.

## Architecture and identity

- Identity is type-derived, not UUID-derived. `EventChannel[TStream]` identity is `EventChannel[TStream.name]`; `Stream[TStream,TEvent]` identity is `Stream[TStream.name,TEvent]`. Use a canonical type-name representation, package-qualified when needed to avoid collisions.
- EventChannel and Stream do not own or generate UUIDs. Do not add UUID recovery, identity catalogs, or deterministic UUID derivation. Event UUIDs identify individual events.
- The public generic relationship is `EventChannel[TStream]`, `Stream[TStream,TEvent]`, and `Event[TEvent]`. One channel family may contain streams with different event types.
- Channel and stream identity strings are metadata on event records. They do not imply separately persisted channel/stream records or instance IDs.
- Event-only persistence: event records are the only persisted record type. There are no channel records, stream records, catalogs, or separately persisted empty entities. Event records carry type-derived channel/stream identity alongside metadata and redacted payload.
- Storage is private. No disk-store type, constructor, interface, parameter, field, option, configuration object, handle, injection point, or consumer-facing API may be exposed. The library privately owns storage initialization and lifecycle.
- The private storage root is a `pulsyflux-stream-data` subdirectory of the process working directory, resolved to an absolute path at initialization.
- No schema-fingerprint or schema/catalog subsystem. Remove fingerprint composition and related machinery rather than replacing it with another catalog.
- Remove retrieval-only `Get()` APIs and map registries. Do not add in-memory lookup caches as substitutes for persisted channel/stream entities. Runtime stream objects may own delivery state.

## Ownership boundaries

- **Event:** typed payload, creation time, redaction policy, payload assignment/validation, serialization/deserialization, and event lifecycle. Event-specific behavior belongs with Event, not a separate schema subsystem.
- **Stream:** concurrency-safe typed delivery of `Event[TEvent]` to subscribers.
- **EventChannel:** typed channel identity and private storage ownership/lifecycle. It does not load or persist a channel entity.
- **Storage:** append-only records, framing/checksum, durability, scan/recovery, and rebuildable metadata. It handles redacted payload bytes, not Go payload types or redaction decisions.
- **Replay:** dispatches historical records through typed stream/channel abstractions; replay is not persisted again by default.

## Payload and redaction invariants

- Every Event has a RedactionPolicy; an empty policy is explicit opt-out. Policy is copied and immutable.
- Validate exclusion paths at policy construction and again before payload assignment. Reject blank, duplicate, malformed, or unresolved paths.
- Redact a serialized copy; never mutate the live payload. Deserialization must not recover excluded values.
- Supported traversal is structs, pointers, and slices/arrays of structs. Maps and embedded-field flattening remain out of scope until deliberately designed and tested.
- Do not introduce an Event envelope. Payload is redacted data directly; event ID, sequence, and timestamp belong in record metadata as appropriate.
- Do not add public Validate/Serialize/Deserialize/TypeName methods or expose payload/time/type metadata as public fields.

## Stream behavior

- Stream delivers only `Event[TEvent]`; distinct event types coexist under one channel family.
- Publish is atomic across subscribers: if any subscriber is full, return `ErrSlowSubscriber` and deliver to none. Publish with no subscribers succeeds.
- Preserve concurrency safety for Subscribe, Publish, cancellation, unsubscribe, and Close.
- Live delivery receives unredacted events; persisted payloads are redacted before writing.
- Individual Publish calls do not write directly to disk. Publish validates/preflights, submits a redacted record to a bounded private queue, then delivers live only after successful queue admission. A full queue applies backpressure; memory must not grow without bound. Successful Publish means queue admission and live delivery, not disk persistence or crash durability. Queue admission and append order are preserved. Crashes may lose queued/unflushed records.

## Disk storage principles

- Keep storage taxonomy explicit: lifecycle, write, read/scan, lookup, replay, maintenance, metadata.
- Prefer append-only segmented files. Make framing, checksum, rollover, sync policy, and recovery boundaries explicit.
- Flush (application buffering) and Sync (OS/filesystem durability) are distinct; document durability windows.
- Storage receives redacted payload bytes and I/O; it does not own payload types or redaction.
- Begin with sequential scan and rebuildable/minimal indexes. Avoid SQL or distributed infrastructure absent concrete requirements.
- Avoid parallel public event/stream/channel/record models without a real boundary.

For implementation mechanics, format, recovery, and platform-specific guarantees, see [Disk Storage Architecture](storage_disk.md). Integration boundaries and current milestones are documented in [Storage Integration Contract](storage_integration_contract.md).

## Approved shared asynchronous writer lifecycle and failure handling (2026-11-22)

- One process-wide private writer is shared by active EventChannels. The manager tracks active-owner count only, not a channel registry. Closing an individual channel releases ownership without stopping the writer while other owners remain. Closing the final owner stops admission, drains accepted queue items, flushes, and closes storage. A later channel can initialize a fresh writer. Final Close returns drain/flush/close errors; it does not imply OS Sync. ErrorStreams remain available until failure notifications are routed, then close.
- A terminal storage failure preserves the first underlying cause, stops persistence admission, and rejects later persisted Publish calls with an error wrapping that cause. Do not silently discard accepted items or claim they persisted.
- The library owns `StreamWriteErrorEvent` and creates it for terminal writer failure. This system event is never persisted, preventing recursive failure. Cause remains discoverable through `errors.Is`/`errors.As`.
- Consumers subscribe through ordinary `Subscription`/`Subscribe`. There is no separate ErrorChannel type or special subscription mechanism. `EventChannel[TStream].Errors(bufferSize)` lazily exposes its channel-owned error stream; consumers subscribe normally.
- Exactly one internal ErrorStream per EventChannel; all error event stream access for that channel uses it. It is not one per business stream and no global error-channel registry exists.
- Internal error publishing bypasses disk persistence but retains normal typed subscriber-delivery semantics.
- Each error subscriber has reserved capacity for the system notification so business-event buffer saturation cannot consume that slot.
- Shared-writer failure is routed to distinct channel owners represented by the failing batch and pending queue. Queue items carry private owner references only for routing; owners are deduplicated. No global channel registry. Later persisted Publish attempts receive the terminal error and channel-local error notification through that path.
- ErrorStream lifetime is scoped to its EventChannel. Close is idempotent and preserves streams through ownership release and failure routing, then closes streams/subscriptions.

## How decisions evolve

Durable architecture decisions are curated here through the documentation review gates in `steering.md`. This file is not a chat log or a full implementation manual. Superseded decisions should be revised or removed with their rationale; current implementation details remain in focused docs, and Git history preserves prior versions.

# System-of-record record scope and lookup

## Purpose

This document describes how typed channel and stream runtime objects define the scope of event records in the system of record.

The central model is hierarchical:

`EventChannel[TStream]` selects a channel-family scope. Within that scope, `Stream[TStream, TEvent]` selects one stream. The records remaining after both selections are the event records for that stream.

This is an architectural description of the internal lookup model. It does **not** introduce a consumer-facing Find/Search API.

## The lookup model

### 1. Resolve the channel scope

The runtime object is:

```go
EventChannel[TStream]
```

`TStream` is the channel-family schema. The schema layer derives its canonical descriptor and deterministic base fingerprint. That fingerprint is the lookup identity for the channel mapping.

Conceptually, the system of record resolves:

```
EventChannel[TStream]
    -> TStream base schema fingerprint
    -> persisted channel mapping
    -> private channel UUID and channel metadata
```

The mapping identifies the logical channel and its persisted record scope. If a mapping already exists, its private identity and metadata are recovered. If it does not exist, a new mapping and private channel UUID are created and persisted.

Once the channel is resolved, the channel scope contains the event records associated with that channel identity.

In set notation:

```
R_channel = all event records whose channel identity is the resolved channel
```

The channel lookup fingerprint and the channel's aggregate record-schema fingerprints are different concepts. The former resolves the channel mapping from `TStream`; the latter describe the channel schema composition used on individual event records.

### 2. Resolve the stream within the channel

The runtime object is:

```go
Stream[TStream, TEvent]
```

The stream schema identity is derived by the schema layer using the shared `TStream` context and the `TEvent` event schema. It is resolved **within the already resolved channel**.

Conceptually:

```
Stream[TStream, TEvent]
    -> stream schema identity
    -> stream mapping within the channel
    -> private stream UUID and stream metadata
```

If the stream mapping exists in that channel, its private identity and metadata are recovered. Otherwise, a new stream mapping and private stream UUID are created and persisted.

The resulting stream scope is the subset of channel records belonging to that stream:

```
R_stream = { r in R_channel | r.stream_identity = resolved_stream_identity }
```

The complete record scope for the runtime stream is therefore `R_stream`.

## Identity scope and uniqueness

The resolved stream identity is **channel-scoped**, not globally meaningful.

The durable identity of a stream is the pair:

```
(resolved channel identity, resolved stream identity)
```

The channel is first resolved from the `TStream` base schema fingerprint. Only after that resolution may the stream schema identity be looked up, and that lookup is constrained to the resolved channel. The same stream schema may therefore be registered in two different channels and resolve to two different persisted stream instances, with distinct private stream UUIDs.

A stream schema fingerprint by itself is not a globally unique stream key. Any storage key, catalog entry, cache, or index used for stream mappings must include the parent channel identity. This rule applies equally to recovery and record lookup.

## Resolve-or-create contract

The internal mapping operation is a durable, atomic **resolve-or-create** operation. It is not a separate consumer-visible create/open choice.

### Channel mapping

For `NewEventChannel[TStream]`:

1. Derive the canonical `TStream` descriptor and its base fingerprint.
2. Ask the schema-mediated system-of-record mapping contract to resolve-or-create the channel mapping for that fingerprint.
3. If an entry exists, compare its stored canonical descriptor with the requested descriptor before restoring its private channel UUID and metadata.
4. If no entry exists, atomically create one mapping and one private channel UUID.
5. Return a usable runtime channel only after resolution/creation has succeeded.

Concurrent constructions for the same canonical `TStream` identity must converge on the same persisted channel mapping and UUID. The storage operation must provide create-if-absent semantics (or equivalent transactional uniqueness); a read followed by an unprotected write is insufficient. A fingerprint hit with a different canonical descriptor is an identity conflict and must fail rather than silently reuse the mapping.

Because durable resolution can fail, channel construction must expose that failure. The current pointer-only `NewEventChannel[TStream]() *EventChannel[TStream]` signature cannot report storage errors; the persistence-aware constructor contract therefore requires an error result, conventionally `(*EventChannel[TStream], error)`. This is an intentional API adjustment to be made when implementation begins, not a change made by this documentation update.

### Stream mapping

For `New[TStream, TEvent](channel, bufferSize)`:

1. Derive the canonical stream descriptor and stream base identity using the `TStream` family context and `TEvent` schema.
2. Resolve-or-create the stream mapping using **both** the already-resolved parent channel identity and the stream schema identity.
3. Validate the stored canonical descriptor on a fingerprint match.
4. Restore the mapped private stream UUID and metadata, or atomically create one mapping and UUID if absent.
5. Register the resulting typed runtime stream in the channel's in-memory registry.

Concurrent attempts to resolve the same stream identity within one channel must converge on one persisted mapping. The existing in-memory duplicate-registration rule remains in force: a second registration of the same event type in the same runtime channel returns `ErrDuplicateStream`. A matching stream schema in a different channel is not a duplicate.

### Failure and consistency rules

- A mapping is either fully persisted and recoverable or treated as absent; partial metadata must not be accepted as a valid mapping.
- Storage errors, descriptor mismatches, corrupt mapping metadata, and UUID conflicts are returned to the constructor as errors. They must not be converted into a fresh identity, because doing so could orphan existing records.
- A failed resolve/create must not leave a partially initialized runtime channel or stream registered in memory.
- Mapping creation must be safe across goroutines and across independent runtime instances/processes that share the same system of record. The storage implementation is responsible for the atomic uniqueness boundary.
- The schema layer derives and validates canonical identities and orchestrates the generic mapping contract. Storage accepts those identities/descriptors and private IDs; it does not reflect over Go types or decide schema equivalence.
- No public Find/Open/Search API is introduced. Constructors remain the consumer-facing recovery boundary.

The precise on-disk catalog layout, locking mechanism, transaction primitive, and storage ownership/lifecycle are intentionally left to the storage design. They must implement these observable guarantees without changing the identity scope above.

### 3. The records in scope

After channel and stream resolution, the system of record has enough identity information to retrieve the event records for that stream. Those records retain their persisted ordering and metadata, including the channel-wide sequence, event-creation timestamp, private instance identities, and schema fingerprints.

The stream's record collection is not a separate event store or a second copy of the data. It is a view over the event records in the channel's system-of-record history, narrowed by stream identity.

A channel may contain multiple streams with different `TEvent` types. The channel-wide record history therefore provides the common scope and ordering; each stream selects its own records from that history.

## Logical filtering versus physical indexing

The two-stage model is the logical definition of scope:

```
TStream schema
    -> resolve channel
    -> channel record collection
    -> resolve stream within channel
    -> stream record collection
```

It does not require storage to physically read every channel record and filter them in application memory.

The storage implementation may use:
- a channel index to locate the channel's record range or segments;
- a composite channel-and-stream index to locate the final stream records directly;
- a sequential scan initially, with rebuildable indexes added when justified.

These are implementation choices. They must preserve the same logical result: a stream lookup is constrained by its resolved parent channel, and it returns exactly the event records belonging to that stream.

A composite index is an optimization, not a change to the identity model. Storage may index and compare persisted identities, but it must not derive Go type schemas or independently decide schema equivalence.

## Identity responsibilities

The lookup model uses three distinct kinds of information:

| Information | Purpose | Owner |
|---|---|---|
| Canonical schema descriptor and fingerprint | Establish structural schema identity and validate a mapping match | `schema.go` |
| Private channel and stream UUIDs | Identify the recovered or newly created persisted instances | Runtime objects and system-of-record mapping |
| Event-record metadata and indexes | Locate, order, and retrieve persisted event records | System of record / storage |

The schema layer is the intermediary between typed runtime objects and the system of record. It derives the schema identities, validates canonical identity on a fingerprint match, and defines the generic resolve-or-create mapping contract.

The channel and stream constructors coordinate this internal resolution. Storage implements durable metadata lookup/write and record retrieval. Storage does not reflect over Go types to derive schemas.

## Transparent construction, not public search

Consumers continue to use the normal typed constructors:

```go
channel := NewEventChannel[TStream]()
stream := New[TStream, TEvent](channel, bufferSize)
```

The constructors resolve existing persisted mappings or create new ones as required. The consumer:
- does not choose between create and open;
- does not supply or inspect private UUIDs;
- does not call a public Find, Open, or Search method;
- does not need to know whether records already exist.

The lookup described here is an internal system-of-record operation used to bind runtime objects to their persisted identities. It is not a new public query API.

## Relationship to event schema fingerprints

Each event record carries six schema fingerprints: Event base/effective, Stream base/effective, and Channel base/effective.

Those fingerprints describe the schema composition applicable to that record. In particular, effective fingerprints can vary with the event's redaction policy. They are valuable for record validation and interpretation, but they are not substitutes for the durable channel and stream mappings.

The mapping keys used to recover the channel and stream runtime objects must remain clearly distinguished from the six per-record fingerprints:
- the channel-family lookup starts from the `TStream` base schema fingerprint;
- the stream mapping is resolved within that channel using the stream schema identity;
- the six event-record fingerprints describe the event, stream, and aggregate channel schemas for that particular record.

A system that can append records containing six fingerprints but cannot recover the channel and stream mappings has not implemented transparent record-scope recovery.

## Invariants

1. A stream's record scope is always subordinate to one resolved channel.
2. A stream lookup must not accidentally select records from another channel, even if the same stream schema appears there.
3. Channel and stream mappings are durable and recoverable independently of the in-memory runtime registry.
4. A fingerprint match is followed by canonical descriptor/identity validation; a hash alone is not treated as proof of equivalence.
5. Private UUIDs remain internal and are restored from mappings rather than supplied by consumers.
6. Channel-wide sequence remains authoritative for historical ordering across streams. Filtering to a stream does not redefine or renumber that order.
7. Indexes are rebuildable accelerators. They do not become the source of schema truth.
8. No public Find/Search/Open API is required to achieve this model.

## Current implementation status

This document records the intended architecture, not a claim that it is already implemented. As of the current code baseline, the disk layer persists event records and their metadata, but durable channel/stream mapping resolution and transparent constructor recovery remain unimplemented. The schema layer currently derives schema identities but does not yet mediate durable mapping resolution.

Implementation must establish the mapping contract and its atomicity/concurrency behaviour before event append and replay are treated as transparently recoverable.

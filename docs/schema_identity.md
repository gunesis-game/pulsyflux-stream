# Schema identity composition

Schema fingerprints describe structural compatibility, not runtime instance identity. Event, Stream, and Channel each have a base fingerprint and an effective fingerprint.

## Event

- Base identity is derived from the canonical serialized structure of `TEvent`.
- Effective identity adds the normalized redaction paths configured on that Event.
- Redaction is event-scoped; different events of the same payload type may use different policies.

## Stream

- A `Stream[TStream, TEvent]` identity includes the canonical `TStream` family context and the Event identity for `TEvent`.
- Stream base identity depends on the Event base identity.
- Stream effective identity depends on the Event effective identity for the particular event being recorded. It can therefore vary between records when event policies vary.

## Channel

- A `EventChannel[TStream]` identity includes the canonical `TStream` context and the set of all registered stream identities.
- Stream dependencies are sorted by base fingerprint and then effective fingerprint before composition, so registration order does not affect the result.
- Channel base identity depends on the complete set of registered stream base identities.
- For a particular record, the active stream contributes its event-specific effective identity. Other registered streams contribute their default effective identity (the identity with an empty redaction policy). This keeps the channel fingerprint aware of the full stream family while allowing each event's own redaction policy to vary independently.

## Record fingerprints

The six record fields are derived from the three runtime identities:

- Event base/effective
- Stream base/effective
- Channel base/effective

Channel UUIDs and event/stream instance IDs remain separate from these structural fingerprints. The current disk record stores the six values, but writer lifecycle and storage ownership are separate concerns.


## Runtime object to system-of-record mappings

Schema fingerprint composition alone is not persistence mapping. The architecture requires two durable mappings, both mediated by the generic contract in `schema.go`:

1. **Channel mapping:** `EventChannel[TStream]` → system-of-record channel metadata and private channel UUID. Derive the canonical `TStream` descriptor and base fingerprint; resolve the existing mapping by that fingerprint, validate the canonical descriptor, and restore its UUID. If absent, atomically create the mapping and a new private UUID.
2. **Stream mapping:** `Stream[TStream, TEvent]` → system-of-record stream metadata and private stream UUID. Within the resolved channel, derive the stream schema identity; resolve and validate its mapping, restoring its UUID, or atomically create a new mapping and UUID.

The consumer always uses the same public constructors. It does not choose create versus open, supply UUIDs, or search storage. Runtime structs retain their private UUIDs after the schema-mediated resolver binds them to the durable records.

The schema layer owns type descriptors, fingerprint derivation, canonical-identity validation, and the generic resolve/create mapping contract. The storage system of record owns durable metadata lookup/write and the underlying index/catalog implementation. Storage must not derive Go type schemas or independently decide schema equivalence.

The six fingerprints stored on each event record describe Event/Stream/Channel base and effective schema identities. They are necessary record metadata, but they do **not** replace the two durable mappings above. A system that can append event records but cannot resolve an `EventChannel` and its `Stream` back to their persisted private identities has not implemented transparent persistence recovery.

### Current implementation status

As of code SHA `4c432a2eefc858c0289a0392bea79684059be846`, this mapping is **not implemented**:
- `schema.go` derives schema identities and handles redacted JSON encoding/decoding, but does not map runtime channel/stream objects to system-of-record entries.
- `NewEventChannel[TStream]` generates a new UUID without checking or persisting a channel mapping.
- `New[TStream, TEvent]` registers an in-memory stream without checking or persisting a stream mapping.
- `storage_disk.go` writes event records with UUIDs and six fingerprints, but does not provide the durable channel/stream mapping catalog or constructor recovery.

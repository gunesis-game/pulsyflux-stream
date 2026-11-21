# Event-record-backed channel and stream identity recovery

## Scope

This document describes the intended recovery model for channel and stream identities. Event records are the system of record. There is no separate durable channel or stream catalog, and empty channels/streams remain ephemeral until an event is appended.

Consumers continue to construct objects through the typed constructors. Storage scanning and identity matching are private implementation details.

## Recovery order

Recovery is hierarchical and event-record-backed:

1. Derive a canonical channel-family descriptor from `TStream` and its deterministic lookup fingerprint.
2. Scan persisted records for that channel-family fingerprint. A matching record establishes the private channel UUID. If no record matches, generate a new runtime UUID; it is not durable until an event is appended.
3. Within the resolved channel UUID, derive the stream schema identity for `TStream, TEvent` and find a matching persisted stream identity. If no record matches, generate a new runtime stream UUID, likewise ephemeral until its first persisted event.
4. Append records with those private UUIDs and schema fingerprints.

Stream identities are scoped by channel: the durable identity is the pair (channel UUID, stream UUID), not a globally unique stream schema fingerprint.

## Fingerprints and record-format implications

The six current record fingerprints have established meanings:

- Event base/effective
- Stream base/effective
- Aggregate channel base/effective

The channel-family lookup fingerprint for `TStream` is distinct from the aggregate channel base fingerprint. The aggregate channel fingerprint depends on registered stream schema identities and cannot reliably identify a channel family before those streams are known.

**Do not repurpose any of the existing six fields.** Record-backed channel recovery requires a separate channel-family fingerprint to be persisted on each event record. This is a seventh fingerprint field and requires a disk record version bump from the current v2 format (to v3), plus validation and tests for encode/decode, missing field rejection, and recovery. This is a design decision derived from the existing schema composition and record structure; implementation remains pending.

A channel-family fingerprint should be derived from the canonical `TStream` descriptor through the shared schema fingerprint mechanism, with format-versioning and canonical descriptor validation. Storage must not independently reflect over Go types or define schema equivalence.

## Record scan and validation

- Scan append-only segments in persisted sequence order.
- For a candidate match, validate the canonical descriptor/identity metadata as well as the fingerprint; hashes are not a mathematical uniqueness guarantee.
- Channel recovery matches the family lookup fingerprint and restores the channel UUID.
- Stream recovery occurs only within the resolved channel UUID and matches the stream base schema identity, restoring the stream UUID.
- Sequence is channel-wide and authoritative; stream replay filters this global sequence without renumbering.
- Existing record CRC/framing, schema fingerprint validation, and payload validity checks remain in force.
- Recovery must fail on corruption or identity conflicts; it must not silently generate a fresh identity and orphan existing records.

## No durable empty entities or public lookup APIs

An empty channel or stream is not represented in the durable store. No separate mapping catalog is introduced merely to preserve empty entities. Consumers do not supply UUIDs, choose create/open, or call public Find/Search/Open APIs. Constructors transparently recover identities when records exist and otherwise create new ephemeral identities.

## Implementation sequence

1. Add channel-family fingerprint derivation to the shared schema layer.
2. Extend disk record schema with the seventh fingerprint and bump format version; update encoding, decoding, size preflight, fixtures, and tests.
3. Add bounded/sequential record lookup for channel family and then channel-scoped stream identity. Ensure concurrent runtime constructor behavior does not register partial objects on error.
4. Connect constructors to record-backed recovery while keeping IDs private and surfacing storage errors.
5. Add restart recovery tests, same-family/different-stream tests, same-stream-schema/different-channel isolation tests, empty-entity ephemeral tests, malformed/corrupt record tests, and concurrency tests.
6. Run required CI on Linux and Windows. Compare aggregate statement and per-function coverage for both platforms from that exact CI run.

## Status

The disk record implementation is currently v2 with six schema fingerprints. Durable channel/stream identity recovery is not implemented. The prior mapping/catalog-based description in this file was obsolete and must not be used as implementation guidance.
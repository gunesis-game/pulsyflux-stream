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

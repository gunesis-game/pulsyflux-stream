# Steering: pulsyflux-stream

## Purpose
Provide a small, concurrency-safe, typed Go event stream, with a clear path toward redactionPolicy-controlled event persistence and replay.

## Core architecture
- **Event owns event semantics.** Typed payload (T), type-derived identity, internal timestamp, redaction redactionPolicy, payload validation, and private serialization/deserialization.
- **Stream owns delivery.** Publish the full in-memory event to subscribers. Streaming does not redact data.
- **Store will own persistence orchestration.** Once a persistence backend exists, the store calls the event's internal serialization method before handing bytes to persistence. It does not inspect payload fields or implement redaction.
- **Persistence owns bytes.** Backends should not know Go generic types, event payload structures, or redaction rules.

## Event redactionPolicy
- Every event is constructed with a redactionPolicy, including an explicitly empty redactionPolicy.
- RedactionPolicy is an exclusion/redaction list of JSON field paths.
- RedactionPolicy paths use JSON field names (tags), not Go field names.
- The redactionPolicy is copied on construction and is immutable after construction.
- RedactionPolicy validation is internal and performed during event construction and again by SetData as a guard.
- Invalid, duplicate, empty, or unresolvable paths fail fast with an error.
- Redaction is applied only to the serialized copy of the payload; it never mutates the live event data.
- Initial path support targets structs, pointers, and slices/arrays of structs. Map traversal and embedded-field flattening need explicit design/tests before being supported.

## Event lifecycle/API
Public API is intended to include:
- NewRedactionRedactionPolicy(exclude ...string) RedactionPolicy
- NewEvent[T](redactionPolicy RedactionPolicy) (Event[T], error)
- (*Event[T]).SetData(T) error
- (*Event[T]).Data() T

Type-name derivation is internal; consumers do not call TypeName.

Serialization/deserialization stay package-private (serialize and deserialize[T]) and are intended for Store persistence integration. The stored envelope includes type, data, and time; redactionPolicy is not stored. Loading requires the caller/store to supply the redactionPolicy compatible with payload type T.

## Implementation status
- The initial redactionPolicy-aware Event API and private serialization/deserialization are being implemented.
- Type name derivation is private to the package.
- Event timestamps are private.
- The current store.go remains only an in-memory stream registry; durable Save/Load is not implemented yet.\n\n## Compatibility and boundaries
- Making Event.Data private and adding redactionPolicy-aware construction/setters is an intentional API change from the original struct-literal event.
- Subscribers continue to receive full, unredacted typed events.
- Current store.go is an in-memory typed stream registry, not a persistence layer. Do not pretend event persistence exists until a persistence backend and store API are implemented.
- A future Store should append serialized records and read by cursor/offset, rather than assume event names are unique or load an unbounded channel history into memory.

## Engineering practices
- Keep dependencies minimal; use standard-library JSON and reflection.
- Add focused tests for redactionPolicy path validation, tagged fields, nested structs, arrays/slices, serialization redaction, deserialize round trips, and invalid data.
- Avoid adding public validation or serialization methods.
- Run go test -race ./..., go vet ./..., and go build ./... before declaring implementation complete. If tool/network limitations prevent these, clearly report that verification is outstanding.
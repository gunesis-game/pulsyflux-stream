# Steering: pulsyflux-stream

## Purpose and scope
Maintain a small, typed, concurrency-safe Go event stream. Event redaction is implemented for the private serialization boundary; durable storage and replay are not implemented. Treat this document as project direction and implementation constraints, not user-facing documentation.

## Ownership boundaries
- **Event:** owns typed payload, derived event type identity, internal timestamp, required redaction policy, payload assignment/validation, and private serialization/deserialization.
- **Stream:** owns concurrent delivery of complete in-memory events. Never redact during Publish.
- **Registry (store.go):** currently only an in-memory typed stream registry; it is not a persistence Store. Keep channel UUID identity separate from registry lifetime.
- **Future persistence Store:** orchestrates calls to private event serialization and backend I/O. Backends handle serialized records only; they must not know Go payload types or redaction rules.

## Redaction invariants
- Every Event is constructed with a RedactionPolicy; an empty policy is explicit opt-out.
- Policy entries are exclusion paths expressed as exact JSON field names/tags.
- Copy policy slices at construction; callers cannot mutate the stored list through their original slice. Keep policy immutable after construction.
- Validate paths in NewEvent[T], and validate again in SetData before assigning payload.
- Reject blank, duplicate, malformed, and unresolved paths.
- Apply redaction only to a serialized JSON copy; never mutate live event data.
- Deserializing serialized redacted data cannot recover excluded values. Do not imply otherwise.
- Current traversal support: structs, pointers, slices/arrays of structs. Maps and embedded-field flattening are out of scope until deliberately designed and tested.

## Public API constraints
Expected event API:
- NewRedactionPolicy(exclude ...string) RedactionPolicy
- NewEvent[T](redactionPolicy RedactionPolicy) (Event[T], error)
- (*Event[T]).SetData(T) error
- (*Event[T]).Data() T

Keep type-name derivation, timestamp, envelope, serialize, deserialize, and validation private. Do not add public Validate/Serialize/Deserialize/TypeName methods or restore public event struct fields for payload/time/type metadata.

## Stream behavior constraints
- Publish sends the full, unredacted event to each active subscriber.
- A full subscriber buffer rejects a Publish atomically with ErrSlowSubscriber; no subscriber receives that rejected event.
- Publishing with zero subscribers succeeds.
- Preserve concurrency safety for Subscribe, Publish, cancellation, unsubscribe, and Close.
- EventChannel UUID is logical identity, not durable storage.

## Implementation and documentation practices
- Prefer standard library JSON/reflection; keep dependencies minimal.
- Add focused tests for policy path validation, JSON tags, nested paths, pointer/slice/array traversal, redaction non-mutation, serialization/deserialization, type mismatch, invalid JSON payloads, and stream concurrency/lifecycle.
- Keep README factual and consumer-focused. Keep this file directive-oriented; avoid duplicating the README walkthrough or API examples.
- Before reporting completion, run gofmt validation, go test -race -cover ./..., go vet ./..., and go build ./.... If execution is unavailable, report verification as unconfirmed.
- CI should print offending filenames when formatting fails; do not hide gofmt output.

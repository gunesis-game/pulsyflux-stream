# Type-derived identity

The package uses type-derived identity strings to identify channel and stream kinds in private persistence records. These strings are routing/type labels, not runtime instance IDs and not schema-compatibility fingerprints.

## Canonical Go type identity

`canonicalTypeIdentity` follows these rules:

- Named Go types use `PkgPath + "." + Name` when the package path is available.
- Named types without a package path use their name.
- Unnamed types fall back to `reflect.Type.String()`.
- A nil `reflect.Type` is represented as `<nil>`.

Named type identities distinguish same-named types from different packages. The unnamed fallback is structural, but its string form is not promised stable across Go/toolchain or source changes.

## Channel and stream labels

- `EventChannel[TStream]`: `EventChannel[<TStream identity>]`
- `Stream[TStream,TEvent]`: `Stream[<TStream identity>,<TEvent identity>]`

These placeholders are illustrative; actual strings are produced by the helpers in `type_identity.go`. The helpers are private and tested directly. A channel or stream instance does not generate UUIDs or own a global identity catalog.

## Persistence boundary

The private `appendEvent[TStream,TEvent]` adapter derives these labels from the generic types, validates the Event, serializes the payload, applies the Event's redaction policy, and writes the redacted bytes with event creation time. The disk record separately retains an event UUID and sequence number.

The record format is version 3. Version 2 and earlier are rejected; no migration or mixed-version segments are supported. Identity labels do not imply structural schema compatibility or payload migration guarantees.

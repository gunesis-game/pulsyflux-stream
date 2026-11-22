> **Pre-production notice:** This Go project is not live, not released, and not in production. Its API and disk format are still under active development; breaking changes are acceptable. Do not infer stability or release status from internal disk-format revision numbers.

# pulsyflux-stream

A small, typed, concurrency-safe event stream for Go.

## Requirements

Go 1.27 or newer.

## Install

```sh
go get github.com/gunesis-game/pulsyflux-stream
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	stream "github.com/gunesis-game/pulsyflux-stream"
)

type PlayerEventFamily struct{}

type PlayerJoined struct {
	PlayerID string `json:"player_id"`
}

func main() {
	channel, err := stream.NewEventChannel[PlayerEventFamily]()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := channel.Close(); err != nil {
			log.Printf("close event channel: %v", err)
		}
	}()

	joined, err := stream.New[PlayerEventFamily, PlayerJoined](channel, 16)
	if err != nil {
		log.Fatal(err)
	}

	sub, err := joined.Subscribe(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	event, err := stream.NewEvent[PlayerJoined](stream.NewRedactionPolicy())
	if err != nil {
		log.Fatal(err)
	}
	if err := event.SetData(PlayerJoined{PlayerID: "player-42"}); err != nil {
		log.Fatal(err)
	}
	if err := joined.Publish(event); err != nil {
		log.Fatal(err)
	}

	received := <-sub.Events()
	fmt.Printf("player joined: %s\n", received.Data().PlayerID)
}
```

## Event channels and streams

- `NewEventChannel[TStream]()` creates a logical grouping based on the `TStream` type and acquires the private shared writer. It returns a channel and an error; storage initialization failures are reported during construction.
- `New[TStream, TEvent](channel, bufferSize)` creates a typed stream in that grouping. It errors for nil/closed channels or non-positive buffer sizes.
- A channel is `EventChannel[TStream]` and can own multiple differently typed streams: `Stream[TStream, TEvent1]`, `Stream[TStream, TEvent2]`, and so on. All streams in one channel share `TStream`; each stream has its own `TEvent`.
- Channel grouping is determined by the `TStream` type; channel instances remain independent and process-local.
- Channel identity is not storage: stream registration is owned by the channel instance and is process-local.
- `EventChannel.Close()` stops channel work, closes active subscriptions on its caller-owned streams, releases private writer ownership, and returns final-owner drain/flush/close errors. It is idempotent.
- `channel.Errors(bufferSize)` lazily returns the channel's single system-only `Stream[ErrorStream, StreamWriteErrorEvent]`; subscribe to it through the ordinary `Subscribe` API. A terminal writer failure is reported once per affected channel, preserves the underlying cause, bypasses persistence, and has one reserved subscriber slot. The error stream remains available through writer release/failure routing and closes with the channel.

## Events and redaction

Every event must be constructed with a `RedactionPolicy`, even when no fields need redaction. Policies list excluded JSON field paths, using JSON tags rather than Go field names. Paths are validated against T when `NewEvent[T]` is called and checked again by `SetData`.

```go
redaction := stream.NewRedactionPolicy("password", "profile.email")
event, err := stream.NewEvent[UserCreated](redaction)
if err != nil {
	log.Fatal(err)
}
if err := event.SetData(user); err != nil {
	log.Fatal(err)
}
```

Redaction is owned by Event and applied to a serialized JSON copy when preparing persisted payloads; the live event and stream subscribers retain the original payload. The redacted payload is what storage persists. Consequently, deserializing a redacted record restores the redacted data, not the original secrets. Serialization/deserialization are package-private and currently have no public persistence API.

Supported policy path traversal is limited to structs, pointers, and slices/arrays of structs. Map traversal and embedded-field flattening are not supported. Paths are exact JSON field names and are case-sensitive. A path must be resolvable; empty, duplicate, and malformed paths are rejected.

### Private persistence internals

The private disk writer currently stores event records using internal format revision 3, with type-derived channel and stream identities, event UUID, sequence, event-creation timestamp, and redacted payload. This is an unreleased development format, not a stable or production format. The private storage implementation is not public API.

Private persistence is integrated into live `Publish`: the event is validated and preflighted against subscriber capacity, its redacted payload is admitted to a bounded asynchronous queue, and the original event is then delivered to subscribers. Successful `Publish` means queue admission and live delivery, not disk durability; queued or unflushed events may be lost on process crash. Terminal writer failures stop later persisted publishes, preserve the first cause, and are routed to affected channel owners through their ErrorStreams. See [disk storage architecture](docs/storage_disk.md), [type-derived identity](docs/type_identity.md), and the [private persistence integration contract](docs/storage_integration_contract.md) for implementation details and limits.

## Delivery semantics

- Each active subscriber receives each successfully published event.
- `Publish` is safe for concurrent callers and sends to all active subscribers.
- Subscribers use bounded buffers set by `New`.
- If any subscriber buffer is full, `Publish` returns `ErrSlowSubscriber` and delivers to none for that call; callers can retry or apply backpressure.
- Publishing with no subscribers succeeds.
- Event timestamps are set during construction and are package-private.
- Subscription close, context cancellation, and stream close are concurrency-safe.
- Publishing after stream shutdown returns `ErrClosed`.

## Development

```sh
go test -race -cover ./...
go vet ./...
go build ./...
```

CI runs formatting validation, race-enabled tests with coverage, vet, and build.

## Releases

Push a semantic version tag (for example, `v1.0.0`). The release workflow tests, vets, and builds before creating a GitHub Release. No generated binary is required for Go module consumers.

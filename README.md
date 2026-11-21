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

type PlayerJoined struct {
	PlayerID string `json:"player_id"`
}

func main() {
	channel := stream.NewEventChannel()
	defer channel.Close()

	joined, err := stream.New[PlayerJoined](channel, 16)
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

- `NewEventChannel()` creates a logical grouping with a random UUID.
- `NewEventChannelWithID(id)` reconstructs a grouping with a non-zero UUID.
- `New[T](channel, bufferSize)` registers a typed stream in that grouping. It errors for nil/invalid/closed channels, non-positive buffer sizes, or duplicate registration of T within the same channel.
- `Get[T](channel)` returns the registered stream or nil if unavailable.
- A channel can own multiple event types. The same event type can be registered in different channel IDs without sharing a stream.
- Channel identity is not storage: the registry is process-local and in-memory.
- `EventChannel.Close()` closes its registered streams and prevents new registrations through that instance. It is idempotent. A new instance can reuse the UUID after closure.

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

Redaction is implemented by the generic schema layer and applied by the event schema adapter during private serialization on a JSON copy; the live event and stream subscribers retain the original payload. The redacted payload is what serialization persists. Consequently, deserializing a redacted record restores the redacted data, not the original secrets. Serialization/deserialization are package-private and currently have no public persistence API.

Supported policy path traversal is limited to structs, pointers, and slices/arrays of structs. Map traversal and embedded-field flattening are not supported. Paths are exact JSON field names and are case-sensitive. A path must be resolvable; empty, duplicate, and malformed paths are rejected.

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

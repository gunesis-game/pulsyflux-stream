# pulsyflux-stream

A small, generic, concurrency-safe event stream for Go.

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
	PlayerID string
}

type PlayerLeft struct {
	PlayerID string
}

func main() {
	// An EventChannel identifies a logical group of related event streams.
	channel := stream.NewEventChannel()
	defer channel.Close()

	joined, err := stream.New[PlayerJoined](channel, 16)
	if err != nil {
		log.Fatal(err)
	}
	left, err := stream.New[PlayerLeft](channel, 16)
	if err != nil {
		log.Fatal(err)
	}

	// Get retrieves the stream for this event type from this channel.
	if stream.Get[PlayerJoined](channel) != joined {
		log.Fatal("expected the registered stream")
	}

	sub, err := joined.Subscribe(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	event, err := stream.NewEvent[PlayerJoined](stream.NewRedactionRedactionPolicy())
	if err != nil {
		log.Fatal(err)
	}
	if err := event.SetData(PlayerJoined{PlayerID: "player-42"}); err != nil {
		log.Fatal(err)
	}
	if err := joined.Publish(event); err != nil {
		log.Fatal(err)
	}

	event = <-sub.Events()
	fmt.Printf("player joined: %s\n", event.Data().PlayerID)

	_ = left // This channel can own other independently typed streams too.
}
```

## Event channels and streams

- `NewEventChannel()` creates a logical event channel with a randomly generated UUID.
- `NewEventChannelWithID(id)` reconstructs a channel using an existing non-zero UUID. Persist this UUID if the same logical channel must be identified again later (for example, by a future replay-capable store).
- `New[T](channel, bufferSize)` creates and registers a typed stream in the
  supplied channel. It returns an error for a nil or closed channel, an invalid
  buffer size, or when that event type is already registered in the channel.
- `Get[T](channel)` retrieves the stream for that event type from that channel.
  It returns `nil` if the channel is nil or no stream has been registered.
- A channel can own streams for many different event types. Each event type can
  be registered only once per channel.
- The in-memory registry keys streams by the composite `StreamKey{ChannelID, EventType}`. Different channel UUIDs can register the same event type without sharing a stream.
- A channel UUID is a logical identity, not a storage location. The current registry is in-memory; a future persistent or distributed backend can use the same UUID without changing the channel identity model.
- `EventChannel.Close()` closes every stream registered under that UUID and prevents further registrations through that channel instance. It is safe to call more than once. A new channel instance can be reconstructed with the same UUID after closure.

## Typed events and redaction policies

Events are created with an explicit redactionPolicy. RedactionPolicy paths use JSON field names (including JSON tags) and are validated against the payload type when the event is constructed. Use an empty redactionPolicy when no redaction is required.

```go
event, err := stream.NewEvent[PlayerJoined](stream.NewRedactionRedactionPolicy())
if err != nil {
    log.Fatal(err)
}
if err := event.SetData(PlayerJoined{PlayerID: "player-42"}); err != nil {
    log.Fatal(err)
}
if err := joined.Publish(event); err != nil {
    log.Fatal(err)
}
```

Redaction is applied to the serialized copy of the payload; subscribers still receive the original typed data. Serialization and deserialization are package-private because they are persistence details. The in-memory stream registry currently does not provide durable storage.

## Semantics

- Each subscriber receives each published event value.
- `Publish` is safe for concurrent callers and delivers to all current
  subscribers.
- Each subscriber has a bounded buffer configured by `New`.
- If any subscriber buffer is full, `Publish` returns `ErrSlowSubscriber` and
  delivers to **none** of the subscribers for that call. The caller can retry,
  apply backpressure, or remove the slow subscriber.
- Publishing with no subscribers succeeds.
- Each event timestamp is set at construction and remains internal to the package.
- `Subscription.Close`, context cancellation, and `Stream.Close` are safe to
  race. Closing a subscription unregisters it and closes its channel.
- Publishing after stream shutdown returns `ErrClosed`.
- `Subscribe` accepts a standard Go `context.Context` for subscription
  cancellation; the `EventChannel` is a separate grouping and lifecycle type.

## Development

```sh
go test -race ./...
go vet ./...
go build ./...
```

## Releases

Push a semantic version tag such as `v1.0.0`. The release workflow runs tests,
vet, and build before creating a GitHub Release for that tag. Go modules use the
tag as the published version; no generated binary is required for consumers.

# pulsyflux-stream

A small, generic, concurrency-safe event stream for Go. The package has no
third-party dependencies.

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

func main() {
	// New constructs and registers the shared stream for PlayerJoined.
	created := stream.New[PlayerJoined](16)

	// Get retrieves that same stream; it does not accept or change a buffer size.
	events := stream.Get[PlayerJoined]()
	if events != created {
		log.Fatal("expected the registered stream")
	}
	defer events.Close()

	sub, err := events.Subscribe(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	if err := events.Publish(stream.Event[PlayerJoined]{
		Name: "player.joined",
		Data: PlayerJoined{PlayerID: "player-42"},
	}); err != nil {
		log.Fatal(err)
	}

	event := <-sub.Events()
	fmt.Printf("%s: %s\n", event.Name, event.Data.PlayerID)
}
```

## Stream store

- `New[T](bufferSize)` creates a stream and registers it for event type `T`.
- `Get[T]()` retrieves the registered stream without creating a new one or
  changing its buffer size. It returns `nil` if `New[T]` has not been called.
- If `New[T]` is called more than once, the most recently created stream is
  registered. Previously returned stream pointers remain independent.
- The store is private to the package; consumers access it through `Get[T]`.

## Semantics

- Each subscriber receives its own copy of each published event value.
- `Publish` is safe for concurrent callers and delivers to all current
  subscribers.
- Each subscriber has a bounded buffer configured by `New`.
- If any subscriber buffer is full, `Publish` returns `ErrSlowSubscriber` and
  delivers to **none** of the subscribers for that call. The caller can retry,
  apply backpressure, or remove the slow subscriber.
- Publishing with no subscribers succeeds.
- A zero `Event.Time` is populated with the current time.
- `Subscription.Close`, context cancellation, and `Stream.Close` are safe to
  race. Closing a subscription unregisters it and closes its channel.
- Publishing after stream shutdown returns `ErrClosed`.
- `New` panics if the buffer size is less than one.

## Development

```sh
go test -race ./...
go vet ./...
go build ./...
```

## Releases

Push a semantic version tag such as `v1.0.0`. The release workflow runs tests,
vet, and build before creating a GitHub Release for the tag. Go modules use the
tag as the published version; no generated binary is required for consumers.

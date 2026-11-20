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

func main() {
	events := stream.New[string](16)
	defer events.Close()

	sub, err := events.Subscribe(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	if err := events.Publish(stream.Event[string]{
		Name: "player.joined",
		Data: "player-42",
	}); err != nil {
		log.Fatal(err)
	}

	event := <-sub.Events()
	fmt.Printf("%s: %s\n", event.Name, event.Data)
}
```

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

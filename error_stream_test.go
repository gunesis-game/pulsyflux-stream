package stream

import (
	"context"
	"errors"
	"testing"
	"time"
)

type errorStreamTestChannel struct{}
type errorStreamTestPayload struct{ Value string }

func TestErrorStreamRoutesTerminalCauseOnce(t *testing.T) {
	channel := mustNewEventChannel[errorStreamTestChannel](t)
	business, err := New[errorStreamTestChannel, errorStreamTestPayload](channel, 1)
	if err != nil {
		t.Fatal(err)
	}
	errorsStream, err := channel.Errors(1)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := errorsStream.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	cause := errors.New("injected disk failure")
	channel.storageWriteFailed(cause)
	select {
	case event := <-sub.Events():
		if !errors.Is(event.Data().Cause, cause) {
			t.Fatalf("cause = %v, want original cause", event.Data().Cause)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for terminal error notification")
	}
	channel.storageWriteFailed(errors.New("later failure"))
	select {
	case event := <-sub.Events():
		t.Fatalf("received duplicate terminal notification: %#v", event.Data())
	default:
	}

	event, err := NewEvent[errorStreamTestPayload](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(errorStreamTestPayload{Value: "not admitted"}); err != nil {
		t.Fatal(err)
	}
	if err := business.Publish(event); !errors.Is(err, cause) {
		t.Fatalf("Publish error = %v, want terminal cause", err)
	}
}

func TestErrorsCreatedAfterFailureReceiveRetainedCause(t *testing.T) {
	channel := mustNewEventChannel[errorStreamTestChannel](t)
	cause := errors.New("failure before subscription")
	channel.storageWriteFailed(cause)
	errorsStream, err := channel.Errors(1)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := errorsStream.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	select {
	case event := <-sub.Events():
		if !errors.Is(event.Data().Cause, cause) {
			t.Fatalf("cause = %v, want %v", event.Data().Cause, cause)
		}
	case <-time.After(time.Second):
		t.Fatal("retained terminal failure was not delivered")
	}
}

func TestErrorStreamRemainsOpenUntilWriterReleaseCompletes(t *testing.T) {
	channel := mustNewEventChannel[errorStreamTestChannel](t)
	errorsStream, err := channel.Errors(1)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := errorsStream.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	cause := errors.New("close-time writer failure")
	release := channel.releaseWriter
	routed := make(chan struct{})
	allowRelease := make(chan struct{})
	channel.releaseWriter = func() error {
		channel.storageWriteFailed(cause)
		close(routed)
		<-allowRelease
		return release()
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- channel.Close()
	}()
	select {
	case <-routed:
	case <-time.After(time.Second):
		t.Fatal("writer release did not route the terminal error")
	}
	select {
	case event, ok := <-sub.Events():
		if !ok {
			t.Fatal("ErrorStream closed before writer release completed")
		}
		if !errors.Is(event.Data().Cause, cause) {
			t.Fatalf("cause = %v, want %v", event.Data().Cause, cause)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for close-time error")
	}
	select {
	case _, ok := <-sub.Events():
		if !ok {
			t.Fatal("ErrorStream closed while writer release was still blocked")
		}
	default:
	}

	close(allowRelease)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("channel Close did not finish after writer release")
	}
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("ErrorStream remained open after channel Close")
		}
	case <-time.After(time.Second):
		t.Fatal("ErrorStream did not close after channel Close")
	}
}

func TestErrorStreamIsSystemOnly(t *testing.T) {
	channel := mustNewEventChannel[errorStreamTestChannel](t)
	errorsStream, err := channel.Errors(1)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewEvent[StreamWriteErrorEvent](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(StreamWriteErrorEvent{Cause: errors.New("manual")}); err != nil {
		t.Fatal(err)
	}
	if err := errorsStream.Publish(event); !errors.Is(err, ErrSystemStream) {
		t.Fatalf("Publish error = %v, want ErrSystemStream", err)
	}
}

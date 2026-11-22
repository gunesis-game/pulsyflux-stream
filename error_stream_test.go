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
	businessSub, err := business.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer businessSub.Close()
	if err := business.Publish(newTestEvent(t, errorStreamTestPayload{Value: "fills business buffer"})); err != nil {
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
	if got := cap(sub.sub.ch); got != 2 {
		t.Fatalf("ErrorStream subscriber capacity = %d, want buffer plus reserved slot (2)", got)
	}

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

type secondErrorStreamTestChannel struct{}

func TestAsyncWriterFailureRoutesToAllChannelErrorStreamsOnce(t *testing.T) {
	channelA, err := NewEventChannel[errorStreamTestChannel]()
	if err != nil {
		t.Fatal(err)
	}
	defer channelA.Close()
	channelB, err := NewEventChannel[secondErrorStreamTestChannel]()
	if err != nil {
		t.Fatal(err)
	}
	defer channelB.Close()

	errorsA, err := channelA.Errors(1)
	if err != nil {
		t.Fatal(err)
	}
	subA, err := errorsA.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer subA.Close()
	errorsB, err := channelB.Errors(1)
	if err != nil {
		t.Fatal(err)
	}
	subB, err := errorsB.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer subB.Close()

	cause := errors.New("shared writer append failure")
	disk := &asyncDiskTestDouble{
		appendStarted: make(chan struct{}),
		appendGate: make(chan struct{}),
		appendErr: cause,
	}
	writer, err := newAsyncDiskWriter(disk, 3, 32, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Submit(asyncWriterItem{owner: channelA, timestamp: time.Now(), payload: []byte("a1")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-disk.appendStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin append")
	}
	if err := writer.Submit(asyncWriterItem{owner: channelA, timestamp: time.Now(), payload: []byte("a2")}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Submit(asyncWriterItem{owner: channelB, timestamp: time.Now(), payload: []byte("b1")}); err != nil {
		t.Fatal(err)
	}
	close(disk.appendGate)
	if err := writer.Close(); err != nil {
		t.Fatalf("writer Close: %v", err)
	}

	for name, sub := range map[string]*Subscription[ErrorStream, StreamWriteErrorEvent]{"channel A": subA, "channel B": subB} {
		select {
		case event := <-sub.Events():
			if !errors.Is(event.Data().Cause, cause) {
				t.Errorf("%s cause = %v, want %v", name, event.Data().Cause, cause)
			}
		case <-time.After(time.Second):
			t.Errorf("%s did not receive terminal failure", name)
		}
		select {
		case event := <-sub.Events():
			t.Errorf("%s received duplicate notification: %#v", name, event.Data())
		default:
		}
	}
}

func TestPublishUnblocksAndReceivesErrorWhenWriterFails(t *testing.T) {
	channel := mustNewEventChannel[errorStreamTestChannel](t)
	defer channel.Close()
	business, err := New[errorStreamTestChannel, errorStreamTestPayload](channel, 1)
	if err != nil {
		t.Fatal(err)
	}
	errorsStream, err := channel.Errors(1)
	if err != nil {
		t.Fatal(err)
	}
	errorSub, err := errorsStream.Subscribe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer errorSub.Close()

	cause := errors.New("blocked publish writer failure")
	disk := &asyncDiskTestDouble{
		appendStarted: make(chan struct{}),
		appendGate: make(chan struct{}),
		appendErr: cause,
	}
	writer, err := newAsyncDiskWriter(disk, 1, 32, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	channel.writer = writer

	first := newTestEvent(t, errorStreamTestPayload{Value: "first"})
	second := newTestEvent(t, errorStreamTestPayload{Value: "second"})
	third := newTestEvent(t, errorStreamTestPayload{Value: "third"})
	if err := business.Publish(first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-disk.appendStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin first append")
	}
	if err := business.Publish(second); err != nil {
		t.Fatal(err)
	}
	thirdResult := make(chan error, 1)
	go func() {
		thirdResult <- business.Publish(third)
	}()
	select {
	case err := <-thirdResult:
		t.Fatalf("third Publish returned before writer failure: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(disk.appendGate)
	select {
	case err := <-thirdResult:
		if !errors.Is(err, cause) {
			t.Fatalf("third Publish error = %v, want %v", err, cause)
		}
	case <-time.After(time.Second):
		t.Fatal("writer failure did not unblock Publish")
	}
	select {
	case event := <-errorSub.Events():
		if !errors.Is(event.Data().Cause, cause) {
			t.Fatalf("ErrorStream cause = %v, want %v", event.Data().Cause, cause)
		}
	case <-time.After(time.Second):
		t.Fatal("Publish-held failure callback deadlocked or failed to notify")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("fake writer Close: %v", err)
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

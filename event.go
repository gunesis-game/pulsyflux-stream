package stream

import (
	"fmt"
	"reflect"
	"time"
)

// RedactionPolicy defines JSON field paths excluded by Event serialization.
// Paths use JSON field names; the generic redaction implementation lives in schema.go.
type RedactionPolicy struct {
	exclude []string
}

// NewRedactionPolicy constructs an immutable-by-convention redaction policy.
// The exclusions are copied and cannot be modified by callers.
func NewRedactionPolicy(exclude ...string) RedactionPolicy {
	return RedactionPolicy{exclude: append([]string(nil), exclude...)}
}

// Event is a typed event with metadata and a payload governed by a redaction
// policy. Data is intentionally private so assignments go through
// SetData.
type Event[T any] struct {
	time time.Time

	data            T
	dataSet         bool
	redactionPolicy RedactionPolicy
}

// NewEvent constructs an event and validates its redactionPolicy against T.
func NewEvent[T any](redactionPolicy RedactionPolicy) (Event[T], error) {
	redactionPolicy.exclude = append([]string(nil), redactionPolicy.exclude...)
	normalized, err := normalizeSchemaRedactionPaths(reflect.TypeOf((*T)(nil)).Elem(), redactionPolicy.exclude)
	if err != nil {
		return Event[T]{}, fmt.Errorf("invalid event redaction policy: %w", err)
	}
	event := Event[T]{time: time.Now(), redactionPolicy: RedactionPolicy{exclude: normalized}}
	return event, nil
}

// SetData validates the event redaction policy and payload serialization before assigning.
func (e *Event[T]) SetData(data T) error {
	if err := e.validate(); err != nil {
		return fmt.Errorf("invalid event redaction policy: %w", err)
	}
	if err := validateSchemaData(data); err != nil {
		return err
	}
	e.data = data
	e.dataSet = true
	return nil
}

// Data returns the original, unredacted typed payload.
func (e *Event[T]) Data() T {
	return e.data
}

// validate checks the event's configured exclusions using generic schema rules.
func (e *Event[T]) validate() error {
	_, err := normalizeSchemaRedactionPaths(reflect.TypeOf((*T)(nil)).Elem(), e.redactionPolicy.exclude)
	return err
}

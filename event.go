package stream

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// RedactionPolicy defines JSON fields that must be removed from an event payload before
// the event is persisted. Paths use JSON field names.
type RedactionPolicy struct {
	exclude []string
}

// NewRedactionRedactionPolicy constructs an immutable-by-convention redaction redactionPolicy.
// The exclusions are copied and cannot be modified by callers.
func NewRedactionRedactionPolicy(exclude ...string) RedactionPolicy {
	return RedactionPolicy{exclude: append([]string(nil), exclude...)}
}

// Event is a typed event with metadata and a payload governed by a redaction
// redactionPolicy. Data is intentionally private so assignments go through SetData.
type Event[T any] struct {
	time time.Time

	data    T
	dataSet bool
	redactionPolicy  RedactionPolicy
}

// NewEvent constructs an event and validates its redactionPolicy against T.
func NewEvent[T any](redactionPolicy RedactionPolicy) (Event[T], error) {
	redactionPolicy.exclude = append([]string(nil), redactionPolicy.exclude...)
	event := Event[T]{time: time.Now(), redactionPolicy: redactionPolicy}
	if err := event.validate(); err != nil {
		return Event[T]{}, fmt.Errorf("invalid event redactionPolicy: %w", err)
	}
	return event, nil
}

// SetData validates the event redactionPolicy and payload serialization before assigning.
func (e *Event[T]) SetData(data T) error {
	if err := e.validate(); err != nil {
		return fmt.Errorf("invalid event redactionPolicy: %w", err)
	}
	if _, err := json.Marshal(data); err != nil {
		return fmt.Errorf("invalid event data: %w", err)
	}
	e.data = data
	e.dataSet = true
	return nil
}

func eventTypeName[T any]() string {
	t := reflect.TypeOf((*T)(nil)).Elem()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Name() != "" {
		return t.Name()
	}
	return t.String()
}

// Data returns the original, unredacted typed payload.
func (e *Event[T]) Data() T {
	return e.data
}

// eventEnvelope is the private persisted representation.
type eventEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
	Time time.Time       `json:"time"`
}

// serialize returns the redacted persisted representation. It never modifies
// the live payload or redactionPolicy.
func (e *Event[T]) serialize() ([]byte, error) {
	if !e.dataSet {
		return nil, ErrUnsetEventData
	}
	raw, err := json.Marshal(e.data)
	if err != nil {
		return nil, fmt.Errorf("marshal event data: %w", err)
	}

	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode event data for redaction: %w", err)
	}
	for _, path := range e.redactionPolicy.exclude {
		redactPath(payload, strings.Split(path, "."))
	}

	redacted, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal redacted event data: %w", err)
	}

	return json.Marshal(eventEnvelope{Type: eventTypeName[T](), Data: redacted, Time: e.time})
}

// deserialize rebuilds an event using the caller-provided redactionPolicy. RedactionPolicy is
// intentionally not stored in the serialized envelope.
func deserialize[T any](raw []byte, redactionPolicy RedactionPolicy) (Event[T], error) {
	event, err := NewEvent[T](redactionPolicy)
	if err != nil {
		return Event[T]{}, fmt.Errorf("invalid event redactionPolicy: %w", err)
	}

	var envelope eventEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return Event[T]{}, fmt.Errorf("deserialize event envelope: %w", err)
	}
	if envelope.Type != eventTypeName[T]() {
		return Event[T]{}, fmt.Errorf("deserialize event: type mismatch, got %q want %q", envelope.Type, eventTypeName[T]())
	}
	if len(envelope.Data) == 0 {
		return Event[T]{}, fmt.Errorf("deserialize event: missing data")
	}

	var data T
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		return Event[T]{}, fmt.Errorf("deserialize event data: %w", err)
	}
	if err := event.SetData(data); err != nil {
		return Event[T]{}, fmt.Errorf("validate event data: %w", err)
	}
	event.time = envelope.Time
	return event, nil
}

// validate checks the redaction paths against the payload type T.
func (e *Event[T]) validate() error {
	t := reflect.TypeOf((*T)(nil)).Elem()
	seen := make(map[string]struct{}, len(e.redactionPolicy.exclude))

	for _, rawPath := range e.redactionPolicy.exclude {
		path := strings.TrimSpace(rawPath)
		if path == "" {
			return fmt.Errorf("empty exclusion path")
		}
		if _, duplicate := seen[path]; duplicate {
			return fmt.Errorf("duplicate exclusion path %q", path)
		}
		seen[path] = struct{}{}

		parts := strings.Split(path, ".")
		for _, part := range parts {
			if part == "" {
				return fmt.Errorf("invalid exclusion path %q", path)
			}
		}
		if err := validatePath(t, parts); err != nil {
			return fmt.Errorf("exclusion %q: %w", path, err)
		}
	}
	return nil
}

func validatePath(t reflect.Type, parts []string) error {
	t = unwrapContainers(t)
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("cannot traverse %s", t.Kind())
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" { // unexported field
			continue
		}
		name, ignored := jsonFieldName(field)
		if ignored || name != parts[0] {
			continue
		}
		if len(parts) == 1 {
			return nil
		}
		return validatePath(field.Type, parts[1:])
	}
	return fmt.Errorf("field %q does not exist on %s", parts[0], t)
}

func unwrapContainers(t reflect.Type) reflect.Type {
	for {
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			t = t.Elem()
		default:
			return t
		}
	}
}

func jsonFieldName(field reflect.StructField) (string, bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", true
	}
	name := field.Name
	if tag != "" {
		tagName := strings.Split(tag, ",")[0]
		if tagName != "" {
			name = tagName
		}
	}
	return name, false
}

func redactPath(value any, parts []string) {
	if len(parts) == 0 {
		return
	}
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			redactPath(item, parts)
		}
	case map[string]any:
		key := parts[0]
		if len(parts) == 1 {
			delete(v, key)
			return
		}
		if child, ok := v[key]; ok {
			redactPath(child, parts[1:])
		}
	}
}

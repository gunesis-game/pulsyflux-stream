package stream

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// eventEnvelope is the private wire representation for persisted events.
type eventEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
	Time time.Time       `json:"time"`
}

// eventTypeName derives the stable in-process type label used by the event envelope.
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

// encodeEventSchema adapts Event[T] to the generic schema layer and returns
// the schema fingerprint and type identity persisted as storage metadata.
func encodeEventSchema[T any](event *Event[T]) ([]byte, string, string, error) {
	if !event.dataSet {
		return nil, "", "", ErrUnsetEventData
	}
	raw, err := json.Marshal(event.data)
	if err != nil {
		return nil, "", "", fmt.Errorf("marshal event data: %w", err)
	}
	redacted, err := redactSchemaJSON(raw, event.redactionPolicy.exclude)
	if err != nil {
		return nil, "", "", fmt.Errorf("redact event data: %w", err)
	}
	typeName := eventTypeName[T]()
	encoded, err := json.Marshal(eventEnvelope{Type: typeName, Data: redacted, Time: event.time})
	if err != nil {
		return nil, "", "", fmt.Errorf("marshal event envelope: %w", err)
	}
	fingerprint := schemaFingerprint(reflect.TypeOf((*T)(nil)).Elem(), schemaEvent)
	return encoded, fingerprint, typeName, nil
}

// decodeEventSchema verifies persisted schema metadata before reconstructing a
// typed event with the current stream's redaction policy.
func decodeEventSchema[T any](raw []byte, fingerprint, persistedTypeName string, policy RedactionPolicy) (Event[T], error) {
	expectedFingerprint := schemaFingerprint(reflect.TypeOf((*T)(nil)).Elem(), schemaEvent)
	if fingerprint != expectedFingerprint {
		return Event[T]{}, fmt.Errorf("deserialize event: schema fingerprint mismatch")
	}

	event, err := NewEvent[T](policy)
	if err != nil {
		return Event[T]{}, fmt.Errorf("invalid event redaction policy: %w", err)
	}

	var envelope eventEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return Event[T]{}, fmt.Errorf("deserialize event envelope: %w", err)
	}
	expectedType := eventTypeName[T]()
	if persistedTypeName != expectedType {
		return Event[T]{}, fmt.Errorf("deserialize event: persisted type mismatch, got %q want %q", persistedTypeName, expectedType)
	}
	if envelope.Type != expectedType {
		return Event[T]{}, fmt.Errorf("deserialize event: type mismatch, got %q want %q", envelope.Type, expectedType)
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

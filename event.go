package stream

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// RedactionPolicy defines JSON field paths excluded by schema-aware serialization.
// Paths use JSON field names. The policy is shared by Event, Stream, and Channel
// schema adapters; each adapter decides privately how the policy is applied.
type RedactionPolicy struct {
	exclude []string
}

// NewRedactionPolicy constructs an immutable-by-convention redaction policy.
// The exclusions are copied and cannot be modified by callers.
func NewRedactionPolicy(exclude ...string) RedactionPolicy {
	return RedactionPolicy{exclude: append([]string(nil), exclude...)}
}

// validateSchemaData checks whether a value can be represented as JSON.
func validateSchemaData[T any](data T) error {
	_, err := json.Marshal(data)
	return err
}

// normalizeSchemaRedactionPaths validates and copies JSON field paths for a
// particular data shape. It is independent of Event, Stream, and Channel.
func normalizeSchemaRedactionPaths(t reflect.Type, paths []string) ([]string, error) {
	normalized := append([]string(nil), paths...)
	seen := make(map[string]struct{}, len(normalized))
	for i, rawPath := range normalized {
		path := strings.TrimSpace(rawPath)
		if path == "" {
			return nil, fmt.Errorf("empty exclusion path")
		}
		if _, duplicate := seen[path]; duplicate {
			return nil, fmt.Errorf("duplicate exclusion path %q", path)
		}
		seen[path] = struct{}{}
		parts := strings.Split(path, ".")
		for _, part := range parts {
			if part == "" {
				return nil, fmt.Errorf("invalid exclusion path %q", path)
			}
		}
		if err := validateSchemaRedactionPath(t, parts); err != nil {
			return nil, fmt.Errorf("exclusion %q: %w", path, err)
		}
		normalized[i] = path
	}
	return normalized, nil
}

func validateSchemaRedactionPath(t reflect.Type, parts []string) error {
	t = unwrapSchemaContainers(t)
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("cannot traverse %s", t.Kind())
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name, ignored := schemaJSONFieldNameWithIgnore(field)
		if ignored || name != parts[0] {
			continue
		}
		if len(parts) == 1 {
			return nil
		}
		return validateSchemaRedactionPath(field.Type, parts[1:])
	}
	return fmt.Errorf("field %q does not exist on %s", parts[0], t)
}

func unwrapSchemaContainers(t reflect.Type) reflect.Type {
	for {
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			t = t.Elem()
		default:
			return t
		}
	}
}

func schemaJSONFieldNameWithIgnore(field reflect.StructField) (string, bool) {
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

// redactSchemaJSON applies JSON field exclusions to a serialized data copy.
// It never mutates the caller's value and can be used by any schema adapter.
func redactSchemaJSON(raw []byte, paths []string) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("decode schema data for redaction: %w", err)
	}
	for _, path := range paths {
		redactSchemaPath(value, strings.Split(path, "."))
	}
	redacted, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal redacted schema data: %w", err)
	}
	return redacted, nil
}

func redactSchemaPath(value any, parts []string) {
	if len(parts) == 0 {
		return
	}
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			redactSchemaPath(item, parts)
		}
	case map[string]any:
		key := parts[0]
		if len(parts) == 1 {
			delete(v, key)
			return
		}
		if child, ok := v[key]; ok {
			redactSchemaPath(child, parts[1:])
		}
	}
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
	normalized, err := normalizeSchemaRedactionPaths(reflect.TypeOf((*T)(nil)).Elem(), redactionPolicy.exclude)
	if err != nil {
		return Event[T]{}, fmt.Errorf("invalid event redaction policy: %w", err)
	}
	event := Event[T]{
		time:            time.Now(),
		redactionPolicy: RedactionPolicy{exclude: normalized},
	}
	return event, nil
}

// SetData validates the event redaction policy and payload serialization before assigning.
func (e *Event[T]) SetData(data T) error {
	if err := e.validate(); err != nil {
		return fmt.Errorf("invalid event redaction policy: %w", err)
	}
	if err := validateSchemaData(data); err != nil {
		return fmt.Errorf("invalid event data: %w", err)
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

package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

const schemaFingerprintVersion = "pulsyflux-schema-v1"

// eventEnvelope is the private wire representation shared by event persistence
// and replay. Redaction policy is supplied by the registered stream at decode time.
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

// validateEventData checks JSON encodability at the schema boundary without
// retaining an encoded copy of the payload.
func validateEventData[T any](data T) error {
	if _, err := json.Marshal(data); err != nil {
		return fmt.Errorf("invalid event data: %w", err)
	}
	return nil
}

// encodeEventSchema owns schema-aware event serialization and returns the
// schema fingerprint and type identity that storage persists as metadata.
// redactPath removes an exclusion path from the JSON-shaped payload copy.
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

func encodeEventSchema[T any](event *Event[T]) ([]byte, string, string, error) {
	if !event.dataSet {
		return nil, "", "", ErrUnsetEventData
	}
	raw, err := json.Marshal(event.data)
	if err != nil {
		return nil, "", "", fmt.Errorf("marshal event data: %w", err)
	}

	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, "", "", fmt.Errorf("decode event data for redaction: %w", err)
	}
	for _, path := range event.redactionPolicy.exclude {
		redactPath(payload, strings.Split(path, "."))
	}

	redacted, err := json.Marshal(payload)
	if err != nil {
		return nil, "", "", fmt.Errorf("marshal redacted event data: %w", err)
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
func decodeEventSchema[T any](raw []byte, fingerprint string, policy RedactionPolicy) (Event[T], error) {
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

type schemaLayer string

const (
	schemaData    schemaLayer = "data"
	schemaEvent   schemaLayer = "event"
	schemaStream  schemaLayer = "stream"
	schemaChannel schemaLayer = "channel"
)

// schemaFingerprint returns a deterministic structural fingerprint for a type at
// the requested logical layer. It is private storage metadata, not an instance ID.
func schemaFingerprint(t reflect.Type, layer schemaLayer) string {
	descriptor := canonicalTypeDescriptor(t)
	dataFingerprint := hashSchema(schemaFingerprintVersion + ":data:" + descriptor)
	switch layer {
	case schemaData:
		return dataFingerprint
	case schemaEvent:
		return hashSchema(schemaFingerprintVersion + ":event:" + dataFingerprint)
	case schemaStream:
		eventFingerprint := hashSchema(schemaFingerprintVersion + ":event:" + dataFingerprint)
		return hashSchema(schemaFingerprintVersion + ":stream:" + eventFingerprint)
	case schemaChannel:
		eventFingerprint := hashSchema(schemaFingerprintVersion + ":event:" + dataFingerprint)
		streamFingerprint := hashSchema(schemaFingerprintVersion + ":stream:" + eventFingerprint)
		return hashSchema(schemaFingerprintVersion + ":channel:" + streamFingerprint)
	default:
		return hashSchema(schemaFingerprintVersion + ":unknown-layer:" + string(layer) + ":" + dataFingerprint)
	}
}

func hashSchema(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// canonicalTypeDescriptor describes exported serialized structure rather than
// Go memory layout. Struct names and package paths are omitted so structurally
// equivalent data types can share a fingerprint. JSON field names and tags,
// field types, and field order normalized by JSON name are included.
func canonicalTypeDescriptor(t reflect.Type) string {
	var b strings.Builder
	seen := make(map[reflect.Type]int)
	nextRef := 0
	writeTypeDescriptor(&b, t, seen, &nextRef)
	return b.String()
}

func writeTypeDescriptor(b *strings.Builder, t reflect.Type, seen map[reflect.Type]int, nextRef *int) {
	if t == nil {
		b.WriteString("nil")
		return
	}
	if ref, ok := seen[t]; ok {
		fmt.Fprintf(b, "ref(%d)", ref)
		return
	}
	ref := *nextRef
	(*nextRef)++
	seen[t] = ref

	fmt.Fprintf(b, "type(%d:%s)", ref, t.Kind())
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.String:
		// Named scalar types are nominally distinct because their names can
		// carry meaning to custom JSON codecs and consumers.
		if t.Name() != "" {
			fmt.Fprintf(b, "named(%s:%s)", t.PkgPath(), t.Name())
		}
	case reflect.Pointer, reflect.Slice:
		writeTypeDescriptor(b, t.Elem(), seen, nextRef)
	case reflect.Array:
		fmt.Fprintf(b, "len(%d)", t.Len())
		writeTypeDescriptor(b, t.Elem(), seen, nextRef)
	case reflect.Map:
		b.WriteString("key{")
		writeTypeDescriptor(b, t.Key(), seen, nextRef)
		b.WriteString("}value{")
		writeTypeDescriptor(b, t.Elem(), seen, nextRef)
		b.WriteByte('}')
	case reflect.Struct:
		fields := make([]reflect.StructField, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" { // omit unexported fields
				continue
			}
			if field.Tag.Get("json") == "-" {
				continue
			}
			fields = append(fields, field)
		}
		sort.SliceStable(fields, func(i, j int) bool {
			return schemaJSONFieldName(fields[i]) < schemaJSONFieldName(fields[j])
		})
		b.WriteString("fields[")
		for _, field := range fields {
			fmt.Fprintf(b, "field(%q,tag(%q){", schemaJSONFieldName(field), field.Tag.Get("json"))
			writeTypeDescriptor(b, field.Type, seen, nextRef)
			b.WriteString("})")
		}
		b.WriteByte(']')
	case reflect.Interface:
		methods := make([]string, 0, t.NumMethod())
		for i := 0; i < t.NumMethod(); i++ {
			method := t.Method(i)
			methods = append(methods, method.Name+":"+method.Type.String())
		}
		sort.Strings(methods)
		fmt.Fprintf(b, "methods(%q)", strings.Join(methods, ";"))
	case reflect.Chan:
		fmt.Fprintf(b, "dir(%d)", t.ChanDir())
		writeTypeDescriptor(b, t.Elem(), seen, nextRef)
	case reflect.Func:
		fmt.Fprintf(b, "func(in:%d,out:%d,variadic:%t)", t.NumIn(), t.NumOut(), t.IsVariadic())
		for i := 0; i < t.NumIn(); i++ {
			writeTypeDescriptor(b, t.In(i), seen, nextRef)
		}
		for i := 0; i < t.NumOut(); i++ {
			writeTypeDescriptor(b, t.Out(i), seen, nextRef)
		}
	case reflect.UnsafePointer:
		b.WriteString("unsafe")
	default:
		fmt.Fprintf(b, "kind(%d)", t.Kind())
	}
}

func schemaJSONFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "" {
		return field.Name
	}
	name := strings.Split(tag, ",")[0]
	if name == "" {
		return field.Name
	}
	return name
}

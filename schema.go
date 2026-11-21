package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const schemaFingerprintVersion = "pulsyflux-schema-v1"

// validateSchemaData checks whether a value can be represented as JSON.
func validateSchemaData[T any](data T) error {
	_, err := json.Marshal(data)
	return err
}

type schemaRedactionPolicy struct {
	exclude []string
}

func newSchemaRedactionPolicy(t reflect.Type, paths []string) (schemaRedactionPolicy, error) {
	normalized, err := normalizeSchemaRedactionPaths(t, paths)
	if err != nil {
		return schemaRedactionPolicy{}, err
	}
	return schemaRedactionPolicy{exclude: normalized}, nil
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
func redactSchemaJSON(raw []byte, policy schemaRedactionPolicy) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("decode schema data for redaction: %w", err)
	}
	for _, path := range policy.exclude {
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

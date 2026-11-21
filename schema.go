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

// schemaIdentity is the shared, private identity contract extended by each
// logical schema layer. Base identifies the unredacted definition; Effective
// identifies that definition after redaction and dependency policies apply.
type schemaIdentity struct {
	Base      string
	Effective string
}

// composeSchemaIdentity is the only fingerprint composition mechanism. Layer
// adapters supply their descriptor, normalized redaction paths, and dependent
// schema identities; they do not implement hashing or policy composition.
func composeSchemaIdentity(layer schemaLayer, descriptor string, redactionPaths []string, dependencies ...schemaIdentity) schemaIdentity {
	var base strings.Builder
	fmt.Fprintf(&base, "%s:base:%s:%s", schemaFingerprintVersion, layer, descriptor)
	var effective strings.Builder
	fmt.Fprintf(&effective, "%s:effective:%s:%s", schemaFingerprintVersion, layer, descriptor)

	for _, dependency := range dependencies {
		base.WriteString(":dependency:")
		base.WriteString(dependency.Base)
		effective.WriteString(":dependency:")
		effective.WriteString(dependency.Effective)
	}

	paths := append([]string(nil), redactionPaths...)
	sort.Strings(paths)
	for _, path := range paths {
		effective.WriteString(":redact:")
		effective.WriteString(path)
	}

	return schemaIdentity{
		Base:      hashSchema(base.String()),
		Effective: hashSchema(effective.String()),
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

// eventSchemaIdentity derives the Event layer identity from the shared schema
// contract. Higher layers compose this identity with their own descriptors.
func eventSchemaIdentity[T any](policy RedactionPolicy) (schemaIdentity, []string, error) {
	t := reflect.TypeOf((*T)(nil)).Elem()
	paths, err := normalizeSchemaRedactionPaths(t, policy.exclude)
	if err != nil {
		return schemaIdentity{}, nil, err
	}
	data := composeSchemaIdentity(schemaData, canonicalTypeDescriptor(t), nil)
	identity := composeSchemaIdentity(schemaEvent, "event:v1", paths, data)
	return identity, paths, nil
}


func streamSchemaIdentity[TStream, TEvent any](event schemaIdentity) schemaIdentity {
	streamContext := reflect.TypeOf((*TStream)(nil)).Elem()
	descriptor := "stream:v1:" + canonicalTypeDescriptor(streamContext)
	return composeSchemaIdentity(schemaStream, descriptor, nil, event)
}

// channelSchemaIdentity composes the channel context with every registered
// stream identity. Sorting by base and then effective identity makes the
// result independent of registration order, including structurally equal
// stream types that have different event-level policies.
func channelSchemaIdentity[TStream any](streams []schemaIdentity) schemaIdentity {
	dependencies := append([]schemaIdentity(nil), streams...)
	sort.Slice(dependencies, func(i, j int) bool {
		if dependencies[i].Base != dependencies[j].Base {
			return dependencies[i].Base < dependencies[j].Base
		}
		return dependencies[i].Effective < dependencies[j].Effective
	})
	channelContext := reflect.TypeOf((*TStream)(nil)).Elem()
	descriptor := "channel:v1:" + canonicalTypeDescriptor(channelContext)
	return composeSchemaIdentity(schemaChannel, descriptor, nil, dependencies...)
}

func schemaFingerprintsFromIdentities(event, stream, channel schemaIdentity) schemaFingerprints {
	return schemaFingerprints{
		EventBase:        event.Base,
		EventEffective:   event.Effective,
		StreamBase:       stream.Base,
		StreamEffective:  stream.Effective,
		ChannelBase:      channel.Base,
		ChannelEffective: channel.Effective,
	}
}

// encodeSchema serializes typed data and applies redaction to a JSON copy.
// The returned bytes are the payload itself, without persistence metadata.
func encodeSchema[T any](data T, policy RedactionPolicy) ([]byte, schemaIdentity, error) {
	identity, paths, err := eventSchemaIdentity[T](policy)
	if err != nil {
		return nil, schemaIdentity{}, fmt.Errorf("validate schema redaction policy: %w", err)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, schemaIdentity{}, fmt.Errorf("marshal schema data: %w", err)
	}
	redacted, err := redactSchemaJSON(raw, paths)
	if err != nil {
		return nil, schemaIdentity{}, fmt.Errorf("redact schema data: %w", err)
	}
	return redacted, identity, nil
}

// decodeSchema validates the expected schema identity before decoding the
// persisted, already-redacted payload. Excluded values cannot be recovered.
func decodeSchema[T any](raw []byte, expected schemaIdentity, policy RedactionPolicy) (T, error) {
	var data T
	identity, _, err := eventSchemaIdentity[T](policy)
	if err != nil {
		return data, fmt.Errorf("validate schema redaction policy: %w", err)
	}
	if identity.Base != expected.Base || identity.Effective != expected.Effective {
		return data, fmt.Errorf("decode schema: schema identity mismatch")
	}
	if len(raw) == 0 || !json.Valid(raw) {
		return data, fmt.Errorf("decode schema: invalid or empty JSON payload")
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return data, fmt.Errorf("decode schema data: %w", err)
	}
	if err := validateSchemaData(data); err != nil {
		return data, fmt.Errorf("validate decoded schema data: %w", err)
	}
	return data, nil
}

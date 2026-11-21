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

type schemaLayer string

const (
	schemaData    schemaLayer = "data"
	schemaEvent   schemaLayer = "event"
	schemaStream  schemaLayer = "stream"
	schemaChannel schemaLayer = "channel"
)

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
	paths, err := normalizeEventRedactionPaths(t, policy.exclude)
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

// Temporary compatibility adapters for the schema identity layer. Event owns the
// payload serialization and redaction implementation.
func encodeSchema[T any](data T, policy RedactionPolicy) ([]byte, schemaIdentity, error) {
	return encodeEventPayload(data, policy)
}

func decodeSchema[T any](raw []byte, expected schemaIdentity, policy RedactionPolicy) (T, error) {
	return decodeEventPayload[T](raw, expected, policy)
}

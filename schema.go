package stream

import (
	"crypto/sha256"
	"encoding/hex"
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

// schemaFingerprint returns a deterministic structural fingerprint for a type at
// the requested logical layer. It is private storage metadata, not an instance ID.
func schemaFingerprint(t reflect.Type, layer schemaLayer) string {
	descriptor := canonicalTypeDescriptor(t)
	identity := fmt.Sprintf("%s:%s:%s", schemaFingerprintVersion, layer, descriptor)
	sum := sha256.Sum256([]byte(identity))
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
	*nextRef++
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
		sort.Slice(fields, func(i, j int) bool {
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

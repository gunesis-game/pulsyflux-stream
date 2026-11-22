package stream

import (
	"fmt"
	"reflect"
)

// canonicalTypeIdentity returns a package-qualified identity for a named Go type.
// For unnamed types, it falls back to reflect's structural type representation.
func canonicalTypeIdentity(t reflect.Type) string {
	if t == nil {
		return "<nil>"
	}
	if name := t.Name(); name != "" {
		if pkg := t.PkgPath(); pkg != "" {
			return pkg + "." + name
		}
		return name
	}
	return t.String()
}

func typeIdentity[T any]() string {
	return canonicalTypeIdentity(reflect.TypeOf((*T)(nil)).Elem())
}

func channelTypeIdentity[TStream any]() string {
	return fmt.Sprintf("EventChannel[%s]", typeIdentity[TStream]())
}

func streamTypeIdentity[TStream, TEvent any]() string {
	return fmt.Sprintf("Stream[%s,%s]", typeIdentity[TStream](), typeIdentity[TEvent]())
}

package stream

import (
	"reflect"
	"testing"
)

func TestCanonicalTypeDescriptorDeterministic(t *testing.T) {
	type payload struct {
		A string `json:"a"`
	}
	typ := reflect.TypeOf(payload{})
	if got, want := canonicalTypeDescriptor(typ), canonicalTypeDescriptor(typ); got != want {
		t.Fatalf("descriptor changed between calls: %q != %q", got, want)
	}
}

func TestSchemaFingerprintChangesWithStructure(t *testing.T) {
	type original struct {
		A string `json:"a"`
	}
	type changed struct {
		A string `json:"a"`
		B string `json:"b"`
	}

	if schemaFingerprint(reflect.TypeOf(original{}), schemaData) == schemaFingerprint(reflect.TypeOf(changed{}), schemaData) {
		t.Fatal("structural change should produce a different fingerprint")
	}
}

func TestSchemaFingerprintIncludesJSONFieldName(t *testing.T) {
	type first struct {
		A string `json:"first"`
	}
	type second struct {
		A string `json:"second"`
	}

	if schemaFingerprint(reflect.TypeOf(first{}), schemaData) == schemaFingerprint(reflect.TypeOf(second{}), schemaData) {
		t.Fatal("JSON field-name change should produce a different fingerprint")
	}
}

func TestSchemaFingerprintNormalizesFieldDeclarationOrder(t *testing.T) {
	type first struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	type second struct {
		B int `json:"b"`
		A int `json:"a"`
	}

	if schemaFingerprint(reflect.TypeOf(first{}), schemaData) != schemaFingerprint(reflect.TypeOf(second{}), schemaData) {
		t.Fatal("field declaration order should not change the structural fingerprint")
	}
}

func TestSchemaFingerprintLayersAreDistinct(t *testing.T) {
	type payload struct {
		A string `json:"a"`
	}
	typ := reflect.TypeOf(payload{})

	data := schemaFingerprint(typ, schemaData)
	event := schemaFingerprint(typ, schemaEvent)
	stream := schemaFingerprint(typ, schemaStream)
	channel := schemaFingerprint(typ, schemaChannel)

	if data == event || event == stream || stream == channel {
		t.Fatal("schema layers should have distinct fingerprints")
	}
}

func TestSchemaFingerprintHandlesRecursiveTypes(t *testing.T) {
	type node struct {
		N *node `json:"n,omitempty"`
	}
	first := schemaFingerprint(reflect.TypeOf(node{}), schemaData)
	second := schemaFingerprint(reflect.TypeOf(node{}), schemaData)
	if first != second {
		t.Fatal("recursive type fingerprint is not deterministic")
	}
}

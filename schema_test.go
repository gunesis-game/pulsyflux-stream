package stream

import "reflect"

func TestCanonicalTypeDescriptorDeterministic(t *testing.T) {
	type payload struct {
		ID   string `json:"id"`
		Name string `json:"name,omitempty"`
	}
	typ := reflect.TypeOf(payload{})
	if got, want := canonicalTypeDescriptor(typ), canonicalTypeDescriptor(typ); got != want {
		t.Fatalf("descriptor changed between calls: %q != %q", got, want)
	}
}

func TestSchemaFingerprintChangesWithStructure(t *testing.T) {
	type original struct {
		ID string `json:"id"`
	}
	type changed struct {
		ID   string `json:"id"`
		Age  int    `json:"age"`
	}

	if schemaFingerprint(reflect.TypeOf(original{}), schemaData) == schemaFingerprint(reflect.TypeOf(changed{}), schemaData) {
		t.Fatal("structural change should produce a different fingerprint")
	}
}

func TestSchemaFingerprintIncludesJSONFieldName(t *testing.T) {
	type first struct {
		Value string `json:"first"`
	}
	type second struct {
		Value string `json:"second"`
	}

	if schemaFingerprint(reflect.TypeOf(first{}), schemaData) == schemaFingerprint(reflect.TypeOf(second{}), schemaData) {
		t.Fatal("JSON field-name change should produce a different fingerprint")
	}
}

func TestSchemaFingerprintNormalizesFieldDeclarationOrder(t *testing.T) {
	type first struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	type second struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	}

	if schemaFingerprint(reflect.TypeOf(first{}), schemaData) != schemaFingerprint(reflect.TypeOf(second{}), schemaData) {
		t.Fatal("field declaration order should not change the structural fingerprint")
	}
}

func TestSchemaFingerprintLayersAreDistinctAndNested(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}
	typ := reflect.TypeOf(payload{})

	data := schemaFingerprint(typ, schemaData)
	event := schemaFingerprint(typ, schemaEvent)
	stream := schemaFingerprint(typ, schemaStream)
	channel := schemaFingerprint(typ, schemaChannel)

	if data == event || event == stream || stream == channel {
		t.Fatal("schema layers should have distinct fingerprints")
	}
	if stream == schemaFingerprint(typ, schemaEvent) || channel == schemaFingerprint(typ, schemaStream) {
		t.Fatal("higher schema layers should include their nested layer identity")
	}
}

func TestSchemaFingerprintHandlesRecursiveTypes(t *testing.T) {
	type node struct {
		Value string `json:"value"`
		Next *node  `json:"next,omitempty"`
	}
	first := schemaFingerprint(reflect.TypeOf(node{}), schemaData)
	second := schemaFingerprint(reflect.TypeOf(node{}), schemaData)
	if first != second {
		t.Fatal("recursive type fingerprint is not deterministic")
	}
}

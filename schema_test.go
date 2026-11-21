package stream

import (
	"encoding/json"
	"reflect"
	"strings"
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

func TestSchemaRedactionAppliesPathsToJSONCopy(t *testing.T) {
	type user struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	}
	type payload struct {
		Users []user `json:"users"`
	}
	original := payload{Users: []user{{Name: "Ada", Password: "secret"}}}
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	redacted, err := redactSchemaJSON(raw, []string{"users.password"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(redacted), "secret") || strings.Contains(string(redacted), "password") {
		t.Fatalf("redacted JSON retained excluded field: %s", redacted)
	}
	if got := original.Users[0].Password; got != "secret" {
		t.Fatalf("schema redaction mutated source value: %q", got)
	}
	if !strings.Contains(string(raw), "secret") {
		t.Fatalf("schema redaction mutated original JSON bytes: %s", raw)
	}
}

func TestNormalizeSchemaRedactionPaths(t *testing.T) {
	type payload struct {
		AccessToken string `json:"access_token"`
	}
	paths, err := normalizeSchemaRedactionPaths(reflect.TypeOf(payload{}), []string{" access_token "})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "access_token" {
		t.Fatalf("normalized paths = %#v", paths)
	}
}

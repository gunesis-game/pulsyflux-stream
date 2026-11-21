package stream

import (
	"strings"
	"testing"
)

func TestNewEventNormalizesRedactionPathWhitespace(t *testing.T) {
	type payload struct { Value string `json:"value"` }
	event, err := NewEvent[payload](NewRedactionPolicy(" value "))
	if err != nil { t.Fatalf("NewEvent rejected surrounding whitespace: %v", err) }
	if err := event.SetData(payload{Value:"secret"}); err != nil { t.Fatal(err) }
	raw, _, err := encodeSchema(event.Data(), event.redactionPolicy)
	if err != nil { t.Fatal(err) }
	if strings.Contains(string(raw), "secret") { t.Fatalf("redaction did not apply: %s", raw) }
}

func TestNewEventRejectsUnknownRedactionPolicyPath(t *testing.T) {
	type payload struct { Email string `json:"email"` }
	if _, err := NewEvent[payload](NewRedactionPolicy("emali")); err == nil { t.Fatal("expected invalid path error") }
}

func TestRedactionPolicyUsesJSONFieldNames(t *testing.T) {
	type payload struct { AccessToken string `json:"access_token"` }
	if _, err := NewEvent[payload](NewRedactionPolicy("access_token")); err != nil { t.Fatalf("JSON field path rejected: %v", err) }
	if _, err := NewEvent[payload](NewRedactionPolicy("AccessToken")); err == nil { t.Fatal("Go field name matched JSON-tagged field") }
}

func TestRedactionPolicyRejectsEmptyAndDuplicatePaths(t *testing.T) {
	type payload struct { Value string `json:"value"` }
	for _, policy := range []RedactionPolicy{NewRedactionPolicy(""), NewRedactionPolicy("value","value"), NewRedactionPolicy("value..nested")} {
		if _, err := NewEvent[payload](policy); err == nil { t.Fatalf("expected validation failure for %#v", policy) }
	}
}

func TestSchemaEncodingWritesDirectRedactedPayload(t *testing.T) {
	type user struct { Name string `json:"name"`; Password string `json:"password"` }
	type payload struct { Users []user `json:"users"` }
	value := payload{Users:[]user{{Name:"Ada",Password:"secret"}}}
	raw, identity, err := encodeSchema(value, NewRedactionPolicy("users.password"))
	if err != nil { t.Fatal(err) }
	if strings.Contains(string(raw), "secret") || strings.Contains(string(raw), "password") { t.Fatalf("redaction leaked: %s", raw) }
	if strings.Contains(string(raw), `"type"`) || strings.Contains(string(raw), `"data"`) || strings.Contains(string(raw), `"time"`) { t.Fatalf("payload contains envelope metadata: %s", raw) }
	if value.Users[0].Password != "secret" { t.Fatal("redaction mutated source data") }
	restored, err := decodeSchema[payload](raw, identity, NewRedactionPolicy("users.password"))
	if err != nil { t.Fatal(err) }
	if restored.Users[0].Name != "Ada" || restored.Users[0].Password != "" { t.Fatalf("unexpected restored payload: %#v", restored) }
}

func TestSchemaDecodeRejectsIdentityMismatch(t *testing.T) {
	type payload struct { Value string `json:"value"` }
	policy := NewRedactionPolicy()
	raw, identity, err := encodeSchema(payload{Value:"ok"}, policy)
	if err != nil { t.Fatal(err) }
	identity.Effective += "x"
	if _, err := decodeSchema[payload](raw, identity, policy); err == nil { t.Fatal("expected schema identity mismatch") }
}

func TestSetDataRejectsNonJSONPayloadAndKeepsPreviousData(t *testing.T) {
	type payload struct { Value string; Func any }
	event, err := NewEvent[payload](NewRedactionPolicy())
	if err != nil { t.Fatal(err) }
	if err := event.SetData(payload{Value:"valid"}); err != nil { t.Fatal(err) }
	if err := event.SetData(payload{Value:"invalid",Func:func(){}}); err == nil { t.Fatal("expected JSON serialization error") }
	if got := event.Data().Value; got != "valid" { t.Fatalf("failed SetData changed existing data: %q", got) }
}

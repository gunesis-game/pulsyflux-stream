package stream

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewEventNormalizesRedactionPathWhitespace(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}

	event, err := NewEvent[payload](NewRedactionPolicy(" value "))
	if err != nil {
		t.Fatalf("NewEvent rejected surrounding whitespace: %v", err)
	}
	if err := event.SetData(payload{Value: "secret"}); err != nil {
		t.Fatal(err)
	}
	raw, _, _, err := encodeEventSchema(&event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret") {
		t.Fatalf("normalized redaction path did not redact data: %s", raw)
	}
}

func TestNewEventRejectsUnknownRedactionPolicyPath(t *testing.T) {
	type payload struct {
		Email string `json:"email"`
	}
	if _, err := NewEvent[payload](NewRedactionPolicy("emali")); err == nil {
		t.Fatal("expected invalid redactionPolicy path error")
	}
}

func TestRedactionPolicyUsesJSONFieldNames(t *testing.T) {
	type payload struct {
		AccessToken string `json:"access_token"`
	}
	if _, err := NewEvent[payload](NewRedactionPolicy("access_token")); err != nil {
		t.Fatalf("JSON field path should be accepted: %v", err)
	}
	if _, err := NewEvent[payload](NewRedactionPolicy("AccessToken")); err == nil {
		t.Fatal("Go field name should not match JSON-tagged field")
	}
}

func TestRedactionPolicyRejectsEmptyAndDuplicatePaths(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}
	for _, redactionPolicy := range []RedactionPolicy{
		NewRedactionPolicy(""),
		NewRedactionPolicy("value", "value"),
		NewRedactionPolicy("value..nested"),
	} {
		if _, err := NewEvent[payload](redactionPolicy); err == nil {
			t.Fatalf("expected redactionPolicy validation failure for %#v", redactionPolicy)
		}
	}
}

func TestEventSerializationRedactsNestedSliceAndDoesNotMutateData(t *testing.T) {
	type user struct {
		Name     string `json:"name"`
		Password string `json:"password"`
	}
	type payload struct {
		Users []user `json:"users"`
	}
	event, err := NewEvent[payload](NewRedactionPolicy("users.password"))
	if err != nil {
		t.Fatal(err)
	}

	value := payload{Users: []user{{Name: "Ada", Password: "secret"}}}
	if err := event.SetData(value); err != nil {
		t.Fatal(err)
	}

	raw, _, _, err := encodeEventSchema(&event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret") || strings.Contains(string(raw), "password") {
		t.Fatalf("serialized output leaked excluded data: %s", raw)
	}
	if got := event.Data().Users[0].Password; got != "secret" {
		t.Fatalf("redaction mutated original data: got %q", got)
	}

	var envelope struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Type != "payload" {
		t.Fatalf("type = %q, want payload", envelope.Type)
	}
}

func TestEventDeserializeRoundTripRedactedData(t *testing.T) {
	type payload struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
	}
	redactionPolicy := NewRedactionPolicy("secret")
	event, err := NewEvent[payload](redactionPolicy)
	if err != nil {
		t.Fatal(err)
	}

	if err := event.SetData(payload{ID: "u1", Secret: "hidden"}); err != nil {
		t.Fatal(err)
	}
	raw, fingerprint, _, err := encodeEventSchema(&event)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := decodeEventSchema[payload](raw, fingerprint, redactionPolicy)
	if err != nil {
		t.Fatal(err)
	}
	got := restored.Data()
	if got.ID != "u1" || got.Secret != "" {
		t.Fatalf("unexpected restored payload: %#v", got)
	}
	if !restored.time.Equal(event.time) {
		t.Fatalf("metadata did not round-trip: restored=%#v original=%#v", restored, event)
	}

	rawAgain, _, _, err := encodeEventSchema(&restored)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rawAgain), "hidden") {
		t.Fatalf("redacted value reappeared after deserialize/serialize: %s", rawAgain)
	}
}

func TestSetDataRejectsNonJSONPayloadAndKeepsPreviousData(t *testing.T) {
	type payload struct {
		Value string
		Func  any
	}
	event, err := NewEvent[payload](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(payload{Value: "valid"}); err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(payload{Value: "invalid", Func: func() {}}); err == nil {
		t.Fatal("expected JSON serialization error")
	}
	if got := event.Data().Value; got != "valid" {
		t.Fatalf("failed SetData changed existing payload: %q", got)
	}
}

func TestEventDeserializeRejectsSchemaFingerprintMismatch(t *testing.T) {
	type payload struct {
		Value string
	}
	event, err := NewEvent[payload](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(payload{Value: "ok"}); err != nil {
		t.Fatal(err)
	}
	raw, fingerprint, _, err := encodeEventSchema(&event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeEventSchema[payload](raw, fingerprint+"x", NewRedactionPolicy()); err == nil {
		t.Fatal("expected schema fingerprint mismatch")
	}
}

package stream

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewEventRejectsUnknownPolicyPath(t *testing.T) {
	type payload struct {
		Email string `json:"email"`
	}
	if _, err := NewEvent[payload](NewPolicy("emali")); err == nil {
		t.Fatal("expected invalid policy path error")
	}
}

func TestPolicyUsesJSONFieldNames(t *testing.T) {
	type payload struct {
		AccessToken string `json:"access_token"`
	}
	if _, err := NewEvent[payload](NewPolicy("access_token")); err != nil {
		t.Fatalf("JSON field path should be accepted: %v", err)
	}
	if _, err := NewEvent[payload](NewPolicy("AccessToken")); err == nil {
		t.Fatal("Go field name should not match JSON-tagged field")
	}
}

func TestPolicyRejectsEmptyAndDuplicatePaths(t *testing.T) {
	type payload struct {
		Value string `json:"value"`
	}
	for _, policy := range []Policy{NewPolicy(""), NewPolicy("value", "value"), NewPolicy("value..nested")} {
		if _, err := NewEvent[payload](policy); err == nil {
			t.Fatalf("expected policy validation failure for %#v", policy)
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
	event, err := NewEvent[payload](NewPolicy("users.password"))
	if err != nil {
		t.Fatal(err)
	}
	event.Name = "users.created"
	value := payload{Users: []user{{Name: "Ada", Password: "secret"}}}
	if err := event.SetData(value); err != nil {
		t.Fatal(err)
	}

	raw, err := event.serialize()
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
		Name string `json:"name"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Name != "users.created" {
		t.Fatalf("name = %q", envelope.Name)
	}
}

func TestEventDeserializeRoundTripRedactedData(t *testing.T) {
	type payload struct {
		ID       string `json:"id"`
		Secret   string `json:"secret"`
	}
	policy := NewPolicy("secret")
	event, err := NewEvent[payload](policy)
	if err != nil {
		t.Fatal(err)
	}
	event.Name = "created"
	if err := event.SetData(payload{ID: "u1", Secret: "hidden"}); err != nil {
		t.Fatal(err)
	}
	raw, err := event.serialize()
	if err != nil {
		t.Fatal(err)
	}

	restored, err := deserialize[payload](raw, policy)
	if err != nil {
		t.Fatal(err)
	}
	got := restored.Data()
	if got.ID != "u1" || got.Secret != "" {
		t.Fatalf("unexpected restored payload: %#v", got)
	}
	if restored.Name != event.Name || !restored.Time.Equal(event.Time) {
		t.Fatalf("metadata did not round-trip: restored=%#v original=%#v", restored, event)
	}

	rawAgain, err := restored.serialize()
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
	event, err := NewEvent[payload](NewPolicy())
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

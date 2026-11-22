package stream

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestEventRedactionAppliesPathsToJSONCopy(t *testing.T) {
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
	redacted, err := redactEventJSON(raw, []string{"users.password"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(redacted), "secret") || strings.Contains(string(redacted), "password") {
		t.Fatalf("redacted JSON retained excluded field: %s", redacted)
	}
	if got := original.Users[0].Password; got != "secret" {
		t.Fatalf("redaction mutated source value: %q", got)
	}
	if !strings.Contains(string(raw), "secret") {
		t.Fatalf("redaction mutated original JSON: %s", raw)
	}
}

func TestNormalizeEventRedactionPaths(t *testing.T) {
	type payload struct {
		AccessToken string `json:"access_token"`
	}
	paths, err := normalizeEventRedactionPaths(reflect.TypeOf(payload{}), []string{" access_token "})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "access_token" {
		t.Fatalf("normalized paths = %#v", paths)
	}
}

func TestNewRedactionPolicyCopiesExclusions(t *testing.T) {
	exclusions := []string{"secret"}
	policy := NewRedactionPolicy(exclusions...)
	exclusions[0] = "public"
	if len(policy.exclude) != 1 || policy.exclude[0] != "secret" {
		t.Fatalf("policy exclusions changed: %#v", policy.exclude)
	}
}

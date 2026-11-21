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

	if hashSchema(canonicalTypeDescriptor(reflect.TypeOf(original{}))) == hashSchema(canonicalTypeDescriptor(reflect.TypeOf(changed{}))) {
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

	if hashSchema(canonicalTypeDescriptor(reflect.TypeOf(first{}))) == hashSchema(canonicalTypeDescriptor(reflect.TypeOf(second{}))) {
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

	if hashSchema(canonicalTypeDescriptor(reflect.TypeOf(first{}))) != hashSchema(canonicalTypeDescriptor(reflect.TypeOf(second{}))) {
		t.Fatal("field declaration order should not change the structural fingerprint")
	}
}

func TestSchemaFingerprintLayersAreDistinct(t *testing.T) {
	type payload struct {
		A string `json:"a"`
	}
	typ := reflect.TypeOf(payload{})

	descriptor := canonicalTypeDescriptor(typ)
	data := composeSchemaIdentity(schemaData, descriptor, nil).Base
	event := composeSchemaIdentity(schemaEvent, descriptor, nil).Base
	stream := composeSchemaIdentity(schemaStream, descriptor, nil).Base
	channel := composeSchemaIdentity(schemaChannel, descriptor, nil).Base

	if data == event || event == stream || stream == channel {
		t.Fatal("schema layers should have distinct fingerprints")
	}
}

func TestSchemaFingerprintHandlesRecursiveTypes(t *testing.T) {
	type node struct {
		N *node `json:"n,omitempty"`
	}
	first := hashSchema(canonicalTypeDescriptor(reflect.TypeOf(node{})))
	second := hashSchema(canonicalTypeDescriptor(reflect.TypeOf(node{})))
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

	redacted, err := redactEventJSON(raw, []string{"users.password"})
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
		t.Fatalf("policy exclusions changed with caller slice: %#v", policy.exclude)
	}
}

func TestSchemaIdentitySeparatesBaseAndEffectiveRedaction(t *testing.T) {
	type payload struct {
		Public string `json:"public"`
		Secret string `json:"secret"`
	}
	typ := reflect.TypeOf(payload{})
	base := composeSchemaIdentity(schemaEvent, canonicalTypeDescriptor(typ), nil)
	redacted := composeSchemaIdentity(schemaEvent, canonicalTypeDescriptor(typ), []string{"secret"})
	otherRedaction := composeSchemaIdentity(schemaEvent, canonicalTypeDescriptor(typ), []string{"public"})

	if base.Base != redacted.Base {
		t.Fatal("redaction must not change the base schema fingerprint")
	}
	if base.Effective == redacted.Effective {
		t.Fatal("redaction must change the effective schema fingerprint")
	}
	if redacted.Effective == otherRedaction.Effective {
		t.Fatal("different redaction policies must produce different effective fingerprints")
	}
}

func TestSchemaIdentityPropagatesDependencyChanges(t *testing.T) {
	firstEvent := composeSchemaIdentity(schemaEvent, "event:v1", []string{"secret"})
	changedEvent := composeSchemaIdentity(schemaEvent, "event:v1", []string{"private"})
	firstStream := composeSchemaIdentity(schemaStream, "stream:v1", nil, firstEvent)
	changedStream := composeSchemaIdentity(schemaStream, "stream:v1", nil, changedEvent)
	firstChannel := composeSchemaIdentity(schemaChannel, "channel:v1", nil, firstStream)
	changedChannel := composeSchemaIdentity(schemaChannel, "channel:v1", nil, changedStream)

	if firstStream.Base != changedStream.Base {
		t.Fatal("stream base fingerprint should depend on the event base schema, not its redaction")
	}
	if firstStream.Effective == changedStream.Effective {
		t.Fatal("stream effective fingerprint should reflect event redaction changes")
	}
	if firstChannel.Effective == changedChannel.Effective {
		t.Fatal("channel effective fingerprint should reflect nested stream changes")
	}
}

func TestStreamSchemaIdentityIncludesStreamContextAndEventDependency(t *testing.T) {
	type streamContextA struct{ Region string }
	type streamContextB struct {
		Region string
		Tenant int
	}
	type payload struct{ Value string }

	event, _, err := eventSchemaIdentity[payload](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	first := streamSchemaIdentity[streamContextA, payload](event)
	second := streamSchemaIdentity[streamContextB, payload](event)
	if first.Base == second.Base || first.Effective == second.Effective {
		t.Fatal("different TStream contexts should produce different stream identities")
	}

	changedEvent := composeSchemaIdentity(schemaEvent, "changed-event", nil)
	changed := streamSchemaIdentity[streamContextA, payload](changedEvent)
	if first.Base == changed.Base {
		t.Fatal("stream base identity should include its TEvent schema dependency")
	}
}

func TestChannelSchemaIdentityIsIndependentOfRegistrationOrder(t *testing.T) {
	type streamContext struct{ Family string }
	type firstPayload struct{ First string }
	type secondPayload struct{ Second int }

	firstChannel := NewEventChannel[streamContext]()
	secondChannel := NewEventChannel[streamContext]()
	defer firstChannel.Close()
	defer secondChannel.Close()

	firstStream, err := New[streamContext, firstPayload](firstChannel, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New[streamContext, secondPayload](firstChannel, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := New[streamContext, secondPayload](secondChannel, 1); err != nil {
		t.Fatal(err)
	}
	secondFirstStream, err := New[streamContext, firstPayload](secondChannel, 1)
	if err != nil {
		t.Fatal(err)
	}

	event, err := NewEvent[firstPayload](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(firstPayload{First: "value"}); err != nil {
		t.Fatal(err)
	}
	firstFingerprints, err := firstStream.schemaFingerprintsFor(event)
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprints, err := secondFirstStream.schemaFingerprintsFor(event)
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprints.ChannelBase != secondFingerprints.ChannelBase ||
		firstFingerprints.ChannelEffective != secondFingerprints.ChannelEffective {
		t.Fatal("channel fingerprints changed with stream registration order")
	}
}

func TestEventPolicyChangesEffectiveFingerprintsPerRecord(t *testing.T) {
	type streamContext struct{}
	type payload struct {
		Public string `json:"public"`
		Secret string `json:"secret"`
		Email  string `json:"email"`
	}

	channel := NewEventChannel[streamContext]()
	defer channel.Close()
	s, err := New[streamContext, payload](channel, 1)
	if err != nil {
		t.Fatal(err)
	}

	first, err := NewEvent[payload](NewRedactionPolicy("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetData(payload{Public: "a", Secret: "s", Email: "e"}); err != nil {
		t.Fatal(err)
	}
	second, err := NewEvent[payload](NewRedactionPolicy("email"))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.SetData(payload{Public: "a", Secret: "s", Email: "e"}); err != nil {
		t.Fatal(err)
	}

	firstFP, err := s.schemaFingerprintsFor(first)
	if err != nil {
		t.Fatal(err)
	}
	secondFP, err := s.schemaFingerprintsFor(second)
	if err != nil {
		t.Fatal(err)
	}
	if !firstFP.valid() || !secondFP.valid() {
		t.Fatal("runtime schema composition did not produce all six fingerprints")
	}
	if firstFP.EventBase != secondFP.EventBase ||
		firstFP.StreamBase != secondFP.StreamBase ||
		firstFP.ChannelBase != secondFP.ChannelBase {
		t.Fatal("event redaction changed a base fingerprint")
	}
	if firstFP.EventEffective == secondFP.EventEffective ||
		firstFP.StreamEffective == secondFP.StreamEffective ||
		firstFP.ChannelEffective == secondFP.ChannelEffective {
		t.Fatal("event redaction did not propagate through all effective fingerprints")
	}
}

func TestAddingStreamChangesChannelBaseIdentity(t *testing.T) {
	type streamContext struct{}
	type firstPayload struct{ First string }
	type secondPayload struct{ Second int }

	channel := NewEventChannel[streamContext]()
	defer channel.Close()
	firstStream, err := New[streamContext, firstPayload](channel, 1)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewEvent[firstPayload](NewRedactionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if err := event.SetData(firstPayload{First: "value"}); err != nil {
		t.Fatal(err)
	}
	before, err := firstStream.schemaFingerprintsFor(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New[streamContext, secondPayload](channel, 1); err != nil {
		t.Fatal(err)
	}
	after, err := firstStream.schemaFingerprintsFor(event)
	if err != nil {
		t.Fatal(err)
	}
	if before.ChannelBase == after.ChannelBase {
		t.Fatal("adding a typed stream should change the channel base identity")
	}
}

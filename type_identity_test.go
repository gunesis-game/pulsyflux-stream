package stream

import (
	"reflect"
	"testing"
)

type identityTestStream struct{}
type identityTestEvent struct{}

func TestCanonicalTypeIdentityUsesPackageQualifiedNames(t *testing.T) {
	want := reflectTypePackageName(t, identityTestStream{})
	got := typeIdentity[identityTestStream]()
	if got != want {
		t.Fatalf("typeIdentity = %q, want %q", got, want)
	}
}

func TestChannelAndStreamIdentitiesAreTypeDerived(t *testing.T) {
	channelWant := "EventChannel[" + typeIdentity[identityTestStream]() + "]"
	if got := channelTypeIdentity[identityTestStream](); got != channelWant {
		t.Fatalf("channel identity = %q, want %q", got, channelWant)
	}

	streamWant := "Stream[" + typeIdentity[identityTestStream]() + "," + typeIdentity[identityTestEvent]() + "]"
	if got := streamTypeIdentity[identityTestStream, identityTestEvent](); got != streamWant {
		t.Fatalf("stream identity = %q, want %q", got, streamWant)
	}
}

func reflectTypePackageName(t *testing.T, value any) string {
	t.Helper()
	typ := reflect.TypeOf(value)
	if typ.PkgPath() == "" {
		return typ.Name()
	}
	return typ.PkgPath() + "." + typ.Name()
}

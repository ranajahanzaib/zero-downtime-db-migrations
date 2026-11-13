package app

import (
	"net/http/httptest"
	"testing"
)

func TestConfigFromEnvDefaultsAndRejectsInvalidMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://demo:demo@localhost/db")
	for _, key := range []string{"APP_VERSION", "READ_MODE", "HTTP_ADDR", "TENANT_HEADER", "INSTANCE_ID"} {
		t.Setenv(key, "")
	}
	c, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.Version != "v1" || c.ReadMode != "legacy" || c.Addr != ":8080" {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	t.Setenv("READ_MODE", "unsafe")
	if _, err = ConfigFromEnv(); err == nil {
		t.Fatal("invalid read mode was accepted")
	}
}

func TestPageOffsetValidation(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  int
		bad   bool
	}{{"", 0, false}, {"?offset=12", 12, false}, {"?offset=-1", 0, true}, {"?offset=1x", 0, true}, {"?offset=1000001", 0, true}} {
		r := httptest.NewRequest("GET", "/api/customers"+tc.query, nil)
		got, err := pageOffset(r)
		if (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
			t.Errorf("query %q: got=%d err=%v", tc.query, got, err)
		}
	}
}

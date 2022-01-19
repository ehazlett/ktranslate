package tracing

import "testing"

func TestNewProvider(t *testing.T) {
	tp, err := NewProvider("", "test-service", "test")
	if err != nil {
		t.Fatal(err)
	}

	if tp == nil {
		t.Fatal("expected provider; received nil")
	}
}

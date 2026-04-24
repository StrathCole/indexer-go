package api

import (
	"testing"
	"time"
)

func TestRuntimeHealthOK(t *testing.T) {
	now := time.Now()

	if !runtimeHealthOK(100, 99, now, now) {
		t.Fatalf("expected one-block lag to be healthy")
	}

	if runtimeHealthOK(100, 98, now, now) {
		t.Fatalf("expected two-block lag to be unhealthy")
	}

	if runtimeHealthOK(100, 100, now.Add(-runtimeHealthStaleAfter-time.Second), now) {
		t.Fatalf("expected stale runtime status to be unhealthy")
	}

	if runtimeHealthOK(0, 0, now, now) {
		t.Fatalf("expected zero-height runtime status to be unhealthy")
	}
}

package model

import (
	"errors"
	"testing"
	"time"
)

func TestCheckValidate(t *testing.T) {
	check := NewCheck("id", "service", "http", "https://example.com/health", 30, 5)
	if err := check.Validate(); err != nil {
		t.Fatalf("valid check rejected: %v", err)
	}
	check.TimeoutSeconds = 30
	if err := check.Validate(); !errors.Is(err, ErrTimeoutGreaterOrEqualInterval) {
		t.Fatalf("expected timeout/interval validation, got %v", err)
	}
	check = NewCheck("id", "service", "tcp", "127.0.0.1:80", 30, 5)
	if err := check.Validate(); err != nil {
		t.Fatalf("valid TCP check rejected: %v", err)
	}
	check.Target = "127.0.0.1"
	if err := check.Validate(); err == nil {
		t.Fatal("TCP target without a port was accepted")
	}
}

func TestStateApplyResultThresholdsAndRedelivery(t *testing.T) {
	check := NewCheck("id", "service", "http", "https://example.com", 30, 5)
	check.FailureThreshold = 2
	state := NewState(check.ID)

	first := Result{RunID: "run-1", Timestamp: time.Now(), Type: "http"}
	if event, applied := state.ApplyResult(check, first); event != nil || !applied {
		t.Fatalf("first failure should only update state: event=%v applied=%v", event, applied)
	}
	second := Result{RunID: "run-2", Timestamp: time.Now(), Type: "http"}
	event, applied := state.ApplyResult(check, second)
	if !applied || event == nil || event.Status != "down" || event.ChangeSeq != 1 {
		t.Fatalf("second failure should transition down: event=%+v applied=%v", event, applied)
	}
	if duplicate, applied := state.ApplyResult(check, second); applied || duplicate == nil || duplicate.RunID != second.RunID {
		t.Fatalf("redelivery should be ignored but return its event for deduplicated publish: event=%+v applied=%v", duplicate, applied)
	}
	if state.ConsecutiveFailures != 2 || state.ChangeSeq != 1 || len(state.Recent) != 2 {
		t.Fatalf("redelivery changed state: %+v", state)
	}

	check.SuccessThreshold = 2
	for i, runID := range []string{"run-3", "run-4"} {
		event, _ = state.ApplyResult(check, Result{RunID: runID, OK: true, Timestamp: time.Now()})
		if i == 0 && event != nil {
			t.Fatalf("first recovery should not transition: %+v", event)
		}
	}
	if event == nil || event.Status != "up" || event.ChangeSeq != 2 {
		t.Fatalf("expected transition up after success threshold: %+v", event)
	}
}

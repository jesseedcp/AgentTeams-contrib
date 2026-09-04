package recovery

import (
	"context"
	"testing"
	"time"
)

// TestRestartRecoveryPersistsQuietWindow simulates the #1177 crash window: a task is
// submitted (status=submitted, continuation pending), the Worker dies before
// sending the completion message, and the Controller restarts. The scanner
// must re-wake the Leader exactly once per quiet window, stop re-waking after
// the Leader accepts, and never re-wake a terminal task.
func TestRestartRecoveryPersistsQuietWindow(t *testing.T) {
	ctx := context.Background()
	s := &fakeStorager{objects: map[string][]byte{
		"shared/tasks/t-1/meta.json": taskMeta("p-1", "t-1", "submitted", "s-1", "d-1"),
	}}
	notifier := &fakeNotifier{failOn: -1}
	scanner := &Scanner{Storager: s, Notifier: notifier, MinInterval: 15 * time.Minute}

	// Controller restart: ScanOnce is called from the leader-elected loop.
	res := scanner.ScanOnce(ctx)
	if res.WakeSent != 1 || len(notifier.wakes) != 1 {
		t.Fatalf("after restart: WakeSent=%d wakes=%d, want 1/1", res.WakeSent, len(notifier.wakes))
	}

	// A new Controller process has no in-memory scanner state. The persisted
	// wake record must still suppress another send inside the quiet window.
	scanner = &Scanner{Storager: s, Notifier: notifier, MinInterval: 15 * time.Minute}
	res = scanner.ScanOnce(ctx)
	if res.WakeSent != 0 {
		t.Fatalf("quiet window: WakeSent=%d, want 0", res.WakeSent)
	}
	if len(notifier.wakes) != 1 {
		t.Fatalf("quiet window: wakes=%d, want still 1", len(notifier.wakes))
	}

	// The Leader finally accepts: submit resolves, continuation flips.
	// The next scan must see 0 candidates.
	s.objects["shared/tasks/t-1/meta.json"] = []byte(`{
		"task_id": "t-1",
		"project_id": "p-1",
		"status": "completed",
		"submission_id": "s-1",
		"continuation": {"status": "resolved", "delivery_id": "d-1", "resolution": "completed"}
	}`)
	res = scanner.ScanOnce(ctx)
	if res.CandidatesFound != 0 {
		t.Fatalf("after accept: CandidatesFound=%d, want 0", res.CandidatesFound)
	}
	if len(notifier.wakes) != 1 {
		t.Fatalf("after accept: wakes=%d, want still 1", len(notifier.wakes))
	}
}

// TestNotificationFailureRetries verifies a notify failure does not write
// the wake record, so the next scan retries (at-least-once delivery).
func TestNotificationFailureRetries(t *testing.T) {
	ctx := context.Background()
	s := &fakeStorager{objects: map[string][]byte{
		"shared/tasks/t-1/meta.json": taskMeta("p-1", "t-1", "submitted", "s-1", "d-1"),
	}}
	notifier := &fakeNotifier{failOn: 0} // first send fails
	scanner := &Scanner{Storager: s, Notifier: notifier, MinInterval: time.Hour}

	res := scanner.ScanOnce(ctx)
	if len(res.Errors) != 1 {
		t.Fatalf("failed notify: Errors=%d, want 1", len(res.Errors))
	}
	if res.WakeSent != 0 {
		t.Fatalf("failed notify: WakeSent=%d, want 0 (no record)", res.WakeSent)
	}
	// No wake record written: the next scan retries.
	if len(s.puts) != 0 {
		t.Fatalf("failed notify wrote wake record: %v", s.puts)
	}

	// Second scan: notifier now succeeds. The scanner calls the notifier
	// again (first attempt failed, so no wake record was written) — that is
	// the intended at-least-once retry.
	notifier.failOn = -1
	res = scanner.ScanOnce(ctx)
	if res.WakeSent != 1 {
		t.Fatalf("retry: WakeSent=%d, want 1", res.WakeSent)
	}
	if len(notifier.wakes) != 2 {
		t.Fatalf("retry: notifier called %d times, want 2 (1 failed + 1 retry)", len(notifier.wakes))
	}
}

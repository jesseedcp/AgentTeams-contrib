package recovery

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss/ossfake"
)

// errMissing mirrors oss.ErrNotExist for the fake.
var errMissing = errors.New("object does not exist")

// fakeStorager wraps a map for the subset the scanner needs.
// ListObjects returns DIRECT child names like ossfake.Memory: for a task
// object "shared/tasks/t-1/meta.json" under prefix "shared/tasks/" it
// returns "t-1/". Children that are not directories are returned as-is.
type fakeStorager struct {
	objects map[string][]byte
	puts    []string
}

func (f *fakeStorager) GetObject(_ context.Context, key string) ([]byte, error) {
	data, ok := f.objects[key]
	if !ok {
		return nil, errMissing
	}
	return data, nil
}

func (f *fakeStorager) PutObject(_ context.Context, key string, data []byte) error {
	f.puts = append(f.puts, key)
	f.objects[key] = append([]byte(nil), data...)
	return nil
}

// listChildren returns full object keys under prefix with the prefix
// STRIPPED, mirroring the real mc ls shape mcLikeOSS emulates: directory
// entries appear as "t-1/" (relative), because mc ls lists immediate
// children. This is the shape production MinIOClient.ListObjects returns.
func (f *fakeStorager) listChildren(prefix string) []string {
	seen := map[string]bool{}
	var out []string
	for k := range f.objects {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		rest := k[len(prefix):]
		slash := indexOf(rest, "/")
		if slash < 0 {
			seen[rest] = true
			continue
		}
		seen[rest[:slash]+"/"] = true
	}
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func indexOf(s string, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func (f *fakeStorager) ListObjects(_ context.Context, prefix string) ([]string, error) {
	return f.listChildren(prefix), nil
}

// fakeNotifier records wakes.
type fakeNotifier struct {
	wakes []WakeMessage
	// failOn decides which wake fails; -1 (default) disables.
	failOn int
}

func (f *fakeNotifier) SendRecoveryWake(_ context.Context, wake WakeMessage) error {
	f.wakes = append(f.wakes, wake)
	if f.failOn == len(f.wakes)-1 {
		return errors.New("matrix send failed")
	}
	return nil
}

func taskMeta(projectID, taskID, status, submissionID, deliveryID string) []byte {
	return []byte(`{
		"task_id": "` + taskID + `",
		"project_id": "` + projectID + `",
		"status": "` + status + `",
		"submission_id": "` + submissionID + `",
		"continuation": {
			"status": "pending",
			"delivery_id": "` + deliveryID + `"
		}
	}`)
}

func TestIsCandidateEligibility(t *testing.T) {
	base := Candidate{
		Status:       "submitted",
		SubmissionID: "s-1",
		Continuation: struct {
			Status     string `json:"status,omitempty"`
			DeliveryID string `json:"delivery_id,omitempty"`
		}{Status: "pending", DeliveryID: "d-1"},
	}
	if !IsCandidate(base) {
		t.Fatal("fully specified candidate must be eligible")
	}
	cases := []struct {
		name string
		mut  func(*Candidate)
	}{
		{"status not submitted", func(c *Candidate) { c.Status = "in_progress" }},
		{"continuation not pending", func(c *Candidate) { c.Continuation.Status = "resolved" }},
		{"missing submission id", func(c *Candidate) { c.SubmissionID = "" }},
		{"missing delivery id", func(c *Candidate) { c.Continuation.DeliveryID = "" }},
	}
	for _, tc := range cases {
		c := base
		tc.mut(&c)
		if IsCandidate(c) {
			t.Errorf("%s: must not be eligible", tc.name)
		}
	}
}

func TestScanOnceFindsPendingSubmission(t *testing.T) {
	ctx := context.Background()
	s := &fakeStorager{objects: map[string][]byte{
		"shared/tasks/t-1/meta.json": taskMeta("p-1", "t-1", "submitted", "s-1", "d-1"),
		"shared/tasks/t-2/meta.json": taskMeta("p-1", "t-2", "in_progress", "s-2", "d-2"),
		"shared/tasks/t-3/meta.json": taskMeta("p-1", "t-3", "submitted", "", ""),
	}}
	notifier := &fakeNotifier{failOn: -1}
	scanner := &Scanner{Storager: s, Notifier: notifier}

	res := scanner.ScanOnce(ctx)
	if res.CandidatesFound != 1 {
		t.Fatalf("CandidatesFound=%d, want 1", res.CandidatesFound)
	}
	if res.WakeSent != 1 {
		t.Fatalf("WakeSent=%d, want 1", res.WakeSent)
	}
	if len(notifier.wakes) != 1 {
		t.Fatalf("wakes=%d, want 1", len(notifier.wakes))
	}
	w := notifier.wakes[0]
	if w.TaskID != "t-1" || w.ProjectID != "p-1" || w.SubmissionID != "s-1" || w.DeliveryID != "d-1" {
		t.Fatalf("unexpected wake: %+v", w)
	}
}

func TestScanOnceKeepsSubmittedTasksIsolated(t *testing.T) {
	ctx := context.Background()
	store := &fakeStorager{objects: map[string][]byte{
		"shared/tasks/t-a/meta.json": []byte(`{
			"task_id":"t-a","project_id":"p-a","room_id":"!room-a:test",
			"status":"submitted","submission_id":"s-a",
			"continuation":{"status":"pending","delivery_id":"d-a"}
		}`),
		"shared/tasks/t-b/meta.json": []byte(`{
			"task_id":"t-b","project_id":"p-b","room_id":"!room-b:test",
			"status":"submitted","submission_id":"s-b",
			"continuation":{"status":"pending","delivery_id":"d-b"}
		}`),
	}}
	notifier := &fakeNotifier{failOn: -1}
	scanner := &Scanner{Storager: store, Notifier: notifier}

	result := scanner.ScanOnce(ctx)
	if result.WakeSent != 2 || len(notifier.wakes) != 2 {
		t.Fatalf("ScanOnce result=%+v wakes=%d, want two", result, len(notifier.wakes))
	}
	wakes := map[string]WakeMessage{}
	for _, wake := range notifier.wakes {
		wakes[wake.TaskID] = wake
	}
	if wake := wakes["t-a"]; wake.ProjectID != "p-a" || wake.RoomID != "!room-a:test" || wake.SubmissionID != "s-a" || wake.DeliveryID != "d-a" {
		t.Fatalf("task a crossed context: %+v", wake)
	}
	if wake := wakes["t-b"]; wake.ProjectID != "p-b" || wake.RoomID != "!room-b:test" || wake.SubmissionID != "s-b" || wake.DeliveryID != "d-b" {
		t.Fatalf("task b crossed context: %+v", wake)
	}
}

func TestDuplicateWakeThrottled(t *testing.T) {
	ctx := context.Background()
	s := &fakeStorager{objects: map[string][]byte{
		"shared/tasks/t-1/meta.json": taskMeta("p-1", "t-1", "submitted", "s-1", "d-1"),
	}}
	notifier := &fakeNotifier{failOn: -1}
	scanner := &Scanner{Storager: s, Notifier: notifier, MinInterval: time.Hour}

	res := scanner.ScanOnce(ctx)
	if res.WakeSent != 1 {
		t.Fatalf("first pass WakeSent=%d, want 1", res.WakeSent)
	}
	res = scanner.ScanOnce(ctx)
	if res.WakeSent != 0 {
		t.Fatalf("second pass WakeSent=%d, want 0 (throttled)", res.WakeSent)
	}
	if len(notifier.wakes) != 1 {
		t.Fatalf("wakes after throttled pass=%d, want 1", len(notifier.wakes))
	}
}

func TestScanCoversTeamPrefix(t *testing.T) {
	ctx := context.Background()
	s := &fakeStorager{objects: map[string][]byte{
		"teams/biz/shared/tasks/t-1/meta.json": taskMeta("p-1", "t-1", "submitted", "s-1", "d-1"),
	}}
	notifier := &fakeNotifier{failOn: -1}
	scanner := &Scanner{Storager: s, Notifier: notifier}

	res := scanner.ScanOnce(ctx)
	if res.CandidatesFound != 1 {
		t.Fatalf("CandidatesFound=%d, want 1", res.CandidatesFound)
	}
	if res.WakeSent != 1 {
		t.Fatalf("WakeSent=%d, want 1", res.WakeSent)
	}
	w := notifier.wakes[0]
	if w.TeamPrefix != "teams/biz/shared/" {
		t.Fatalf("TeamPrefix=%q, want teams/biz/shared/", w.TeamPrefix)
	}
}

func TestScanCoversTeamPrefixFromFullObjectListing(t *testing.T) {
	ctx := context.Background()
	store := ossfake.NewMemory()
	if err := store.PutObject(ctx, "teams/biz/shared/tasks/t-1/meta.json", taskMeta("p-1", "t-1", "submitted", "s-1", "d-1")); err != nil {
		t.Fatalf("put task meta: %v", err)
	}
	notifier := &fakeNotifier{failOn: -1}
	scanner := &Scanner{Storager: store, Notifier: notifier}

	result := scanner.ScanOnce(ctx)
	if result.CandidatesFound != 1 || result.WakeSent != 1 {
		t.Fatalf("ScanOnce result=%+v, want one team-scoped wake", result)
	}
	if got := notifier.wakes[0].TeamPrefix; got != "teams/biz/shared/" {
		t.Fatalf("TeamPrefix=%q, want teams/biz/shared/", got)
	}
}

func TestTerminalTaskNeverWoken(t *testing.T) {
	ctx := context.Background()
	s := &fakeStorager{objects: map[string][]byte{
		"shared/tasks/t-1/meta.json": taskMeta("p-1", "t-1", "completed", "s-1", "d-1"),
	}}
	notifier := &fakeNotifier{failOn: -1}
	scanner := &Scanner{Storager: s, Notifier: notifier}

	res := scanner.ScanOnce(ctx)
	if res.WakeSent != 0 {
		t.Fatalf("terminal task woken, WakeSent=%d", res.WakeSent)
	}
}

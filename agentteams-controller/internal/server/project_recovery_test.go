package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/matrix"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss/ossfake"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/recovery"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// recordingMatrix captures SendMessageContentAsAdmin calls.
type recordingMatrix struct {
	matrix.Client
	calls int
	sent  []struct {
		RoomID        string
		TransactionID string
		Content       map[string]interface{}
	}
	err error
}

func (r *recordingMatrix) SendMessageContentAsAdmin(_ context.Context, roomID, transactionID string, content map[string]interface{}) (string, error) {
	r.calls++
	if r.err != nil {
		return "", r.err
	}
	r.sent = append(r.sent, struct {
		RoomID        string
		TransactionID string
		Content       map[string]interface{}
	}{roomID, transactionID, content})
	return "$recovery-event", nil
}

// TestDispatcherSendsWakeWithLeaderMention covers the happy path: a team
// project, leader resolved via Team CR, wake sent to the task's room with the
// completion line.
func TestDispatcherSendsWakeWithLeaderMention(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "teams/biz/shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putPendingTaskMeta(t, mem, "teams/biz/shared/tasks/t-1/meta.json")

	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Spec: v1beta1.TeamSpec{
			WorkerMembers: []v1beta1.TeamWorkerRef{{Name: "lead", Role: "team_leader"}},
		},
		Status: v1beta1.TeamStatus{
			Members: []v1beta1.TeamMemberStatus{
				{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
			},
		},
	}
	leader := &v1beta1.Worker{ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"}}
	k8s := newTestK8sClient(t, team, leader)
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, k8s, "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID:    "p-1",
		TaskID:       "t-1",
		TeamPrefix:   "teams/biz/shared/",
		RoomID:       "!task-room:matrix.local",
		ResultPath:   "shared/tasks/t-1/result.md",
		SubmissionID: "s-1",
		DeliveryID:   "d-1",
	})
	if err != nil {
		t.Fatalf("SendRecoveryWake: %v", err)
	}
	if len(mtx.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(mtx.sent))
	}
	got := mtx.sent[0]
	if got.RoomID != "!task-room:matrix.local" {
		t.Fatalf("room=%q, want task room", got.RoomID)
	}
	if got.TransactionID != "teamharness-completion-d-1" {
		t.Fatalf("transaction id=%q, want stable delivery id", got.TransactionID)
	}
	want := "@lead:matrix.local TASK_COMPLETED: t-1 - Result: shared/tasks/t-1/result.md\n" +
		"Project: p-1\nSubmission: s-1\nDelivery: d-1"
	if body, _ := got.Content["body"].(string); body != want {
		t.Fatalf("body=%q, want %q", body, want)
	}
	mentions, ok := got.Content["m.mentions"].(map[string]interface{})
	if !ok {
		t.Fatalf("content has no m.mentions: %v", got.Content)
	}
	ids, _ := mentions["user_ids"].([]string)
	if len(ids) != 1 || ids[0] != "@lead:matrix.local" {
		t.Fatalf("m.mentions.user_ids=%v, want [@lead:matrix.local]", ids)
	}
}

func TestDispatcherWakesSleepingLeaderBeforeSending(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "teams/biz/shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putPendingTaskMeta(t, mem, "teams/biz/shared/tasks/t-1/meta.json")

	sleeping := "Sleeping"
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Status: v1beta1.TeamStatus{Members: []v1beta1.TeamMemberStatus{
			{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
		}},
	}
	leader := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{State: &sleeping},
	}
	k8s := newTestK8sClient(t, team, leader)
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, k8s, "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID:    "p-1",
		TaskID:       "t-1",
		TeamPrefix:   "teams/biz/shared/",
		RoomID:       "!task-room:matrix.local",
		SubmissionID: "s-1",
		DeliveryID:   "d-1",
	})
	if err != nil {
		t.Fatalf("SendRecoveryWake: %v", err)
	}
	var updated v1beta1.Worker
	if err := k8s.Get(ctx, client.ObjectKey{Name: "lead", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get Leader Worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Running" {
		t.Fatalf("Leader desired state=%q, want Running", updated.Spec.DesiredState())
	}
	if len(mtx.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(mtx.sent))
	}
}

// TestDispatcherNoLeaderRetriesWithoutSending verifies that an unresolved
// Leader is never recorded as a successful wake. A later scan can retry after
// the Team controller publishes the real Leader identity.
func TestDispatcherNoLeaderRetriesWithoutSending(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "teams/biz/shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putPendingTaskMeta(t, mem, "teams/biz/shared/tasks/t-1/meta.json")
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Spec: v1beta1.TeamSpec{
			WorkerMembers: []v1beta1.TeamWorkerRef{{Name: "lead", Role: "team_leader"}},
		},
		// Status.Members empty: leader not observed yet.
	}
	k8s := newTestK8sClient(t, team)
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, k8s, "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID:    "p-1",
		TaskID:       "t-1",
		TeamPrefix:   "teams/biz/shared/",
		RoomID:       "!task-room:matrix.local",
		SubmissionID: "s-1",
		DeliveryID:   "d-1",
	})
	if err == nil {
		t.Fatal("SendRecoveryWake with no leader must remain retryable")
	}
	if len(mtx.sent) != 0 {
		t.Fatalf("sent %d messages without a real Leader, want 0", len(mtx.sent))
	}
}

func TestDispatcherResolvesEffectiveTeamName(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "teams/runtime-team/shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "runtime-team",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putPendingTaskMeta(t, mem, "teams/runtime-team/shared/tasks/t-1/meta.json")
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "team-cr", Namespace: "default"},
		Spec:       v1beta1.TeamSpec{TeamName: "runtime-team"},
		Status: v1beta1.TeamStatus{Members: []v1beta1.TeamMemberStatus{
			{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
		}},
	}
	leader := &v1beta1.Worker{ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"}}
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, newTestK8sClient(t, team, leader), "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID:    "p-1",
		TaskID:       "t-1",
		TeamPrefix:   "teams/runtime-team/shared/",
		RoomID:       "!task-room:matrix.local",
		SubmissionID: "s-1",
		DeliveryID:   "d-1",
	})
	if err != nil {
		t.Fatalf("SendRecoveryWake with effective team name: %v", err)
	}
	if len(mtx.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(mtx.sent))
	}
}

// TestDispatcherTerminalNodeNeverWoken verifies the design-doc condition 5:
// a project node already decided (completed) is never re-woken even when the
// TaskMeta continuation is stale.
func TestDispatcherTerminalNodeNeverWoken(t *testing.T) {
	for _, status := range []string{"completed", "revision", "blocked", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			mem := ossfake.NewMemory()
			putProjectMeta(t, mem, "shared/projects/p-1/meta.json", fmt.Sprintf(`{
				"project_id": "p-1",
				"team_id": "biz-team",
				"tasks": [{"task_id": "t-1", "status": %q}]
			}`, status))
			putPendingTaskMeta(t, mem, "shared/tasks/t-1/meta.json")
			mtx := &recordingMatrix{}
			d := NewDispatcher(mem, mtx, newTestK8sClient(t), "default")

			err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
				ProjectID: "p-1", TaskID: "t-1", RoomID: "!task-room:matrix.local",
				SubmissionID: "s-1", DeliveryID: "d-1",
			})
			if !errors.Is(err, recovery.ErrTaskDecided) {
				t.Fatalf("SendRecoveryWake terminal node: err=%v, want ErrTaskDecided", err)
			}
			if len(mtx.sent) != 0 {
				t.Fatalf("terminal node woken: sent %d messages, want 0", len(mtx.sent))
			}
		})
	}
}

func TestDispatcherTerminalLoopNodeNeverWoken(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"loop": {"tasks": [{"task_id": "t-1", "status": "blocked"}]}
	}`)
	putPendingTaskMeta(t, mem, "shared/tasks/t-1/meta.json")
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, newTestK8sClient(t), "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID: "p-1", TaskID: "t-1", RoomID: "!task-room:matrix.local",
		SubmissionID: "s-1", DeliveryID: "d-1",
	})
	if !errors.Is(err, recovery.ErrTaskDecided) {
		t.Fatalf("SendRecoveryWake terminal loop node: err=%v, want ErrTaskDecided", err)
	}
	if len(mtx.sent) != 0 {
		t.Fatalf("terminal loop node woken: sent %d messages, want 0", len(mtx.sent))
	}
}

func TestDispatcherCompletedProjectNeverWoken(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"status": "completed",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putPendingTaskMeta(t, mem, "shared/tasks/t-1/meta.json")
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Status: v1beta1.TeamStatus{Members: []v1beta1.TeamMemberStatus{
			{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
		}},
	}
	leader := &v1beta1.Worker{ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"}}
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, newTestK8sClient(t, team, leader), "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID: "p-1", TaskID: "t-1", RoomID: "!task-room:matrix.local",
		SubmissionID: "s-1", DeliveryID: "d-1",
	})
	if !errors.Is(err, recovery.ErrTaskDecided) {
		t.Fatalf("SendRecoveryWake completed project: err=%v, want ErrTaskDecided", err)
	}
	if len(mtx.sent) != 0 {
		t.Fatalf("completed project woken: sent %d messages, want 0", len(mtx.sent))
	}
}

func TestDispatcherMissingProjectNodeNeverWoken(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"status": "active",
		"tasks": []
	}`)
	putPendingTaskMeta(t, mem, "shared/tasks/t-1/meta.json")
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Status: v1beta1.TeamStatus{Members: []v1beta1.TeamMemberStatus{
			{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
		}},
	}
	leader := &v1beta1.Worker{ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"}}
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, newTestK8sClient(t, team, leader), "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID: "p-1", TaskID: "t-1", RoomID: "!task-room:matrix.local",
		SubmissionID: "s-1", DeliveryID: "d-1",
	})
	if !errors.Is(err, recovery.ErrTaskDecided) {
		t.Fatalf("SendRecoveryWake missing project node: err=%v, want ErrTaskDecided", err)
	}
	if len(mtx.sent) != 0 {
		t.Fatalf("missing project node woken: sent %d messages, want 0", len(mtx.sent))
	}
}

func TestDispatcherRechecksTaskBeforeSending(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"status": "active",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putProjectMeta(t, mem, "shared/tasks/t-1/meta.json", `{
		"project_id": "p-1",
		"task_id": "t-1",
		"status": "completed",
		"submission_id": "s-1",
		"continuation": {"status": "resolved", "delivery_id": "d-1", "resolution": "completed"}
	}`)
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Status: v1beta1.TeamStatus{Members: []v1beta1.TeamMemberStatus{
			{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
		}},
	}
	leader := &v1beta1.Worker{ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"}}
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, newTestK8sClient(t, team, leader), "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID: "p-1", TaskID: "t-1", RoomID: "!task-room:matrix.local",
		SubmissionID: "s-1", DeliveryID: "d-1",
	})
	if !errors.Is(err, recovery.ErrTaskDecided) {
		t.Fatalf("SendRecoveryWake resolved task: err=%v, want ErrTaskDecided", err)
	}
	if len(mtx.sent) != 0 {
		t.Fatalf("resolved task woken: sent %d messages, want 0", len(mtx.sent))
	}
}

func TestDispatcherRechecksTaskRoomBeforeSending(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putProjectMeta(t, mem, "shared/tasks/t-1/meta.json", `{
		"project_id": "p-1",
		"task_id": "t-1",
		"status": "submitted",
		"submission_id": "s-1",
		"room_id": "!new-task-room:matrix.local",
		"continuation": {"status": "pending", "delivery_id": "d-1"}
	}`)
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Status: v1beta1.TeamStatus{Members: []v1beta1.TeamMemberStatus{
			{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
		}},
	}
	leader := &v1beta1.Worker{ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"}}
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, newTestK8sClient(t, team, leader), "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID: "p-1", TaskID: "t-1", RoomID: "!old-task-room:matrix.local",
		SubmissionID: "s-1", DeliveryID: "d-1",
	})
	if !errors.Is(err, recovery.ErrTaskDecided) {
		t.Fatalf("SendRecoveryWake stale room: err=%v, want ErrTaskDecided", err)
	}
	if len(mtx.sent) != 0 {
		t.Fatalf("stale assignment room received %d messages, want 0", len(mtx.sent))
	}
}

// TestDispatcherNoRoomSkips verifies a task without its assignment room is
// skipped instead of falling back to the project's source room.
func TestDispatcherNoRoomSkips(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"source_room_id": "!request-room:matrix.local",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putProjectMeta(t, mem, "shared/tasks/t-1/meta.json", `{
		"project_id": "p-1",
		"task_id": "t-1",
		"status": "submitted",
		"submission_id": "s-1",
		"continuation": {"status": "pending", "delivery_id": "d-1"}
	}`)
	mtx := &recordingMatrix{}
	d := NewDispatcher(mem, mtx, newTestK8sClient(t), "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID:    "p-1",
		TaskID:       "t-1",
		SubmissionID: "s-1",
		DeliveryID:   "d-1",
	})
	if !errors.Is(err, recovery.ErrTaskDecided) {
		t.Fatalf("SendRecoveryWake without room: err=%v, want ErrTaskDecided", err)
	}
	if len(mtx.sent) != 0 {
		t.Fatalf("sent %d messages without a room, want 0", len(mtx.sent))
	}
}

// TestDispatcherSendErrorPropagates verifies delivery failures surface as
// errors so the scan pass records them and the wake record is not written.
func TestDispatcherSendErrorPropagates(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putPendingTaskMeta(t, mem, "shared/tasks/t-1/meta.json")
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Status: v1beta1.TeamStatus{Members: []v1beta1.TeamMemberStatus{
			{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
		}},
	}
	leader := &v1beta1.Worker{ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"}}
	mtx := &recordingMatrix{err: context.DeadlineExceeded}
	d := NewDispatcher(mem, mtx, newTestK8sClient(t, team, leader), "default")

	err := d.SendRecoveryWake(ctx, recovery.WakeMessage{
		ProjectID:    "p-1",
		TaskID:       "t-1",
		RoomID:       "!task-room:matrix.local",
		ResultPath:   "shared/tasks/t-1/result.md",
		SubmissionID: "s-1",
		DeliveryID:   "d-1",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SendRecoveryWake error=%v, want Matrix deadline error", err)
	}
	if mtx.calls != 1 {
		t.Fatalf("Matrix calls=%d, want 1", mtx.calls)
	}
}

// TestRecoveryFlowFromTaskMetaToMatrix crosses the full PR2 boundary: the
// scanner discovers canonical TaskMeta, the dispatcher wakes a sleeping
// Leader, and TuwunelClient emits the structured Matrix event.
func TestRecoveryFlowFromTaskMetaToMatrix(t *testing.T) {
	ctx := context.Background()
	mem := ossfake.NewMemory()
	putProjectMeta(t, mem, "shared/projects/p-1/meta.json", `{
		"project_id": "p-1",
		"team_id": "biz-team",
		"tasks": [{"task_id": "t-1", "status": "submitted"}]
	}`)
	putPendingTaskMeta(t, mem, "shared/tasks/t-1/meta.json")

	var sentPath string
	var sentContent map[string]interface{}
	homeserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_matrix/client/v3/login":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "admin-token"})
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/send/m.room.message/"):
			sentPath = r.URL.Path
			_ = json.NewDecoder(r.Body).Decode(&sentContent)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"event_id":"$recovery-event"}`))
		default:
			t.Errorf("unexpected Matrix request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer homeserver.Close()

	sleeping := "Sleeping"
	team := &v1beta1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "biz-team", Namespace: "default"},
		Status: v1beta1.TeamStatus{Members: []v1beta1.TeamMemberStatus{
			{Name: "lead", Role: "team_leader", MatrixUserID: "@lead:matrix.local"},
		}},
	}
	leader := &v1beta1.Worker{
		ObjectMeta: metav1.ObjectMeta{Name: "lead", Namespace: "default"},
		Spec:       v1beta1.WorkerSpec{State: &sleeping},
	}
	k8s := newTestK8sClient(t, team, leader)
	matrixClient := matrix.NewTuwunelClient(matrix.Config{
		ServerURL:     homeserver.URL,
		Domain:        "matrix.local",
		AdminUser:     "admin",
		AdminPassword: "admin-password",
	}, homeserver.Client())
	dispatcher := NewDispatcher(mem, matrixClient, k8s, "default")
	scanner := &recovery.Scanner{
		Storager:    mem,
		Notifier:    dispatcher,
		MinInterval: time.Hour,
	}

	result := scanner.ScanOnce(ctx)
	if len(result.Errors) != 0 || result.WakeSent != 1 {
		t.Fatalf("ScanOnce result=%+v, want one successful wake", result)
	}
	if !strings.HasSuffix(sentPath, "/teamharness-completion-d-1") {
		t.Fatalf("Matrix path=%q, want stable delivery transaction", sentPath)
	}
	mentions, ok := sentContent["m.mentions"].(map[string]interface{})
	if !ok {
		t.Fatalf("Matrix event has no structured mention: %v", sentContent)
	}
	userIDs, _ := mentions["user_ids"].([]interface{})
	if len(userIDs) != 1 || userIDs[0] != "@lead:matrix.local" {
		t.Fatalf("Matrix mentions=%v, want Leader identity", userIDs)
	}
	if body, _ := sentContent["body"].(string); !strings.Contains(body, "Submission: s-1\nDelivery: d-1") {
		t.Fatalf("Matrix body lacks durable submission identity: %q", body)
	}
	var updated v1beta1.Worker
	if err := k8s.Get(ctx, client.ObjectKey{Name: "lead", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("get Leader Worker: %v", err)
	}
	if updated.Spec.DesiredState() != "Running" {
		t.Fatalf("Leader desired state=%q, want Running", updated.Spec.DesiredState())
	}
}

// putProjectMeta writes a project meta.json into the in-memory OSS.
func putProjectMeta(t *testing.T, mem *ossfake.Memory, key, body string) {
	t.Helper()
	if err := mem.PutObject(context.Background(), key, []byte(body)); err != nil {
		t.Fatalf("put project meta: %v", err)
	}
}

func putPendingTaskMeta(t *testing.T, mem *ossfake.Memory, key string) {
	t.Helper()
	putProjectMeta(t, mem, key, `{
		"project_id": "p-1",
		"task_id": "t-1",
		"status": "submitted",
		"submission_id": "s-1",
		"room_id": "!task-room:matrix.local",
		"result_path": "shared/tasks/t-1/result.md",
		"continuation": {"status": "pending", "delivery_id": "d-1"}
	}`)
}

// newTestK8sClient builds a fake K8s client with the given runtime objects.
func newTestK8sClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	ro := make([]runtime.Object, 0, len(objs))
	for _, o := range objs {
		ro = append(ro, o)
	}
	return fake.NewClientBuilder().WithScheme(newProjectTestScheme(t)).WithRuntimeObjects(ro...).Build()
}

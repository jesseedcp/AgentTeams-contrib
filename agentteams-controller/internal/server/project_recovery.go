package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/oss"
	"github.com/agentscope-ai/AgentTeams/agentteams-controller/internal/recovery"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type recoveryMatrixSender interface {
	SendMessageContentAsAdmin(ctx context.Context, roomID, transactionID string, content map[string]interface{}) (string, error)
}

// Dispatcher resolves the Leader for recovered submissions and sends the
// wake-up through Matrix. It owns three concerns at the boundary the design
// doc sets for PR2: read the project's team id (storage), resolve the team
// leader's Matrix identity (Kubernetes Team CR), and deliver the completion
// line (Matrix admin send).
type Dispatcher struct {
	OSS    oss.StorageClient
	Matrix recoveryMatrixSender
	K8s    client.Client
	NS     string
}

// NewDispatcher wires a recovery.Notifier against the Controller's
// storage/Matrix/Kubernetes clients.
func NewDispatcher(o oss.StorageClient, m recoveryMatrixSender, k client.Client, ns string) *Dispatcher {
	return &Dispatcher{OSS: o, Matrix: m, K8s: k, NS: ns}
}

// SendRecoveryWake implements recovery.Notifier. It never auto-accepts: it
// only re-wakes the Leader with a TASK_COMPLETED line so the Leader can run
// check_task / accept_task_result itself.
func (d *Dispatcher) SendRecoveryWake(ctx context.Context, wake recovery.WakeMessage) error {
	if strings.TrimSpace(wake.SubmissionID) == "" || strings.TrimSpace(wake.DeliveryID) == "" {
		return fmt.Errorf("recovery wake requires submission_id and delivery_id")
	}
	stillPending, err := d.taskStillPending(ctx, wake)
	if err != nil {
		return err
	}
	if !stillPending {
		return recovery.ErrTaskDecided
	}
	roomID := strings.TrimSpace(wake.RoomID)
	if roomID == "" {
		// Do not fall back to the project's requester/source room. The
		// submission stays pending and a later scan can retry once its task
		// room exists.
		return recovery.ErrTaskDecided
	}
	leader, err := d.leaderForProject(ctx, wake)
	if err != nil {
		return err
	}
	if err := d.ensureLeaderRunning(ctx, leader.Name); err != nil {
		return err
	}
	// Waking a sleeping Leader crosses a Kubernetes write boundary. The
	// normal acceptance/cancellation path may win while that update is in
	// flight, so fence both canonical files again immediately before the
	// Matrix send. ProjectMeta is checked separately because TeamHarness
	// commits the project decision before repairing TaskMeta.
	stillPending, err = d.taskStillPending(ctx, wake)
	if err != nil {
		return err
	}
	if !stillPending {
		return recovery.ErrTaskDecided
	}
	project, err := d.readProjectMeta(ctx, wake.ProjectID, wake.TeamPrefix)
	if err != nil {
		return err
	}
	if project == nil || projectNodeTerminal(project, wake.TaskID) {
		return recovery.ErrTaskDecided
	}
	body := recovery.CompletionBody(leader.MatrixUserID, wake)
	content := map[string]interface{}{
		"msgtype": "m.text",
		"body":    body,
	}
	// Structured mention is what triggers appservice wake dispatch for a
	// sleeping Leader; without it the text alone reaches only a running
	// agent. The recovery wake must survive either case, so include
	// m.mentions whenever the Leader id resolved.
	if leader.MatrixUserID != "" {
		content["m.mentions"] = map[string]interface{}{
			"user_ids": []string{leader.MatrixUserID},
		}
	}
	// Name the transaction after the logical recovery delivery, not this scan
	// attempt. Controller retries reuse it and collapse to one Matrix event.
	// The normal Worker notification uses a different Matrix identity, so the
	// two paths remain intentionally at-least-once and may both wake the Leader.
	transactionID := "teamharness-completion-" + wake.DeliveryID
	if _, err := d.Matrix.SendMessageContentAsAdmin(ctx, roomID, transactionID, content); err != nil {
		return fmt.Errorf("send recovery wake to %s: %w", roomID, err)
	}
	return nil
}

func (d *Dispatcher) taskStillPending(ctx context.Context, wake recovery.WakeMessage) (bool, error) {
	key := "shared/tasks/" + wake.TaskID + "/meta.json"
	if wake.TeamPrefix != "" {
		key = wake.TeamPrefix + "tasks/" + wake.TaskID + "/meta.json"
	}
	data, err := d.OSS.GetObject(ctx, key)
	if err != nil {
		return false, fmt.Errorf("read task meta %s: %w", key, err)
	}
	var current recovery.Candidate
	if err := json.Unmarshal(data, &current); err != nil {
		return false, fmt.Errorf("decode task meta %s: %w", key, err)
	}
	if current.ProjectID != wake.ProjectID || current.TaskID != wake.TaskID ||
		current.SubmissionID != wake.SubmissionID || current.Continuation.DeliveryID != wake.DeliveryID ||
		strings.TrimSpace(current.RoomID) != strings.TrimSpace(wake.RoomID) {
		return false, nil
	}
	return recovery.IsCandidate(current), nil
}

// projectNodeTerminal reports whether the project or its matching task node
// has already reached a terminal decision. A missing node also fails closed.
func projectNodeTerminal(project *projectMeta, taskID string) bool {
	if project.Status == "completed" || project.Status == "blocked" {
		return true
	}
	tasks := project.Tasks
	if project.Loop != nil {
		tasks = append(tasks, project.Loop.Tasks...)
	}
	for _, t := range tasks {
		if t.TaskID == taskID {
			return isTerminalTaskStatus(t.Status)
		}
	}
	// Eligibility requires a matching non-terminal project node. Absence is
	// not evidence that the task is safe to wake.
	return true
}

// leaderForProject resolves the team leader's Matrix user ID from the Team CR
// named by the project's team_id. Missing project/team/Leader state remains a
// retryable error: sending an unstructured @leader placeholder would not wake
// anyone, but would make the scanner record a false success.
type leaderTarget struct {
	Name         string
	MatrixUserID string
}

func (d *Dispatcher) leaderForProject(ctx context.Context, wake recovery.WakeMessage) (leaderTarget, error) {
	project, err := d.readProjectMeta(ctx, wake.ProjectID, wake.TeamPrefix)
	if err != nil {
		return leaderTarget{}, err
	}
	if project == nil {
		return leaderTarget{}, fmt.Errorf("project %s is unavailable", wake.ProjectID)
	}
	// Re-check the project/task fence in the same read used for Leader
	// resolution. This avoids sending from an older project snapshot after a
	// normal acceptance wins the race.
	if projectNodeTerminal(project, wake.TaskID) {
		return leaderTarget{}, recovery.ErrTaskDecided
	}
	if strings.TrimSpace(project.TeamID) == "" {
		return leaderTarget{}, fmt.Errorf("project %s has no team_id", wake.ProjectID)
	}
	team, err := d.teamByID(ctx, project.TeamID)
	if err != nil {
		return leaderTarget{}, err
	}
	for i := range team.Status.Members {
		m := team.Status.Members[i]
		if m.Role == "team_leader" && m.Name != "" && m.MatrixUserID != "" {
			return leaderTarget{Name: m.Name, MatrixUserID: m.MatrixUserID}, nil
		}
	}
	return leaderTarget{}, fmt.Errorf("team %s has no observed Leader Matrix identity", project.TeamID)
}

// teamByID accepts both Team metadata.name and the effective runtime name
// stored in TeamSpec.teamName. TeamHarness writes the latter into project
// metadata, and the two names are allowed to differ.
func (d *Dispatcher) teamByID(ctx context.Context, teamID string) (v1beta1.Team, error) {
	var team v1beta1.Team
	if err := d.K8s.Get(ctx, client.ObjectKey{Name: teamID, Namespace: d.NS}, &team); err == nil {
		return team, nil
	} else if !apierrors.IsNotFound(err) {
		return v1beta1.Team{}, fmt.Errorf("get team %s: %w", teamID, err)
	}

	var teams v1beta1.TeamList
	if err := d.K8s.List(ctx, &teams, client.InNamespace(d.NS)); err != nil {
		return v1beta1.Team{}, fmt.Errorf("resolve effective team %s: %w", teamID, err)
	}
	var matched *v1beta1.Team
	for i := range teams.Items {
		candidate := &teams.Items[i]
		if candidate.Spec.EffectiveTeamName(candidate.Name) != teamID {
			continue
		}
		if matched != nil {
			return v1beta1.Team{}, fmt.Errorf("effective team name %s is ambiguous", teamID)
		}
		matched = candidate
	}
	if matched == nil {
		return v1beta1.Team{}, fmt.Errorf("team %s is unavailable", teamID)
	}
	return *matched, nil
}

func (d *Dispatcher) ensureLeaderRunning(ctx context.Context, name string) error {
	running := "Running"
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var worker v1beta1.Worker
		if err := d.K8s.Get(ctx, client.ObjectKey{Name: name, Namespace: d.NS}, &worker); err != nil {
			return fmt.Errorf("get Leader Worker %s: %w", name, err)
		}
		switch worker.Spec.DesiredState() {
		case "Running":
			return nil
		case "Sleeping":
			worker.Spec.State = &running
			return d.K8s.Update(ctx, &worker)
		default:
			return fmt.Errorf("Leader Worker %s is %s", name, worker.Spec.DesiredState())
		}
	})
}

// readProjectMeta loads the owning project meta.json. The project may live
// under the global prefix or a team-scoped prefix; the task's storage scope
// picks which one to read so a team project never silently falls back to a
// global project with the same id.
func (d *Dispatcher) readProjectMeta(ctx context.Context, projectID, teamPrefix string) (*projectMeta, error) {
	if projectID == "" {
		return nil, nil
	}
	key := "shared/projects/" + projectID + "/meta.json"
	if teamPrefix != "" {
		key = teamPrefix + "projects/" + projectID + "/meta.json"
	}
	data, err := d.OSS.GetObject(ctx, key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read project meta %s: %w", key, err)
	}
	var meta projectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("decode project meta %s: %w", key, err)
	}
	if meta.ProjectID != projectID {
		return nil, nil
	}
	return &meta, nil
}

var _ recovery.Notifier = (*Dispatcher)(nil)

// Package recovery implements the Controller-side leader-elected scanner
// that re-wakes TeamHarness Leaders for submitted-but-not-yet-accepted tasks
// (AgentTeams issue #1177).
//
// PR1 (#1183) made the "leader needs to look at this result" fact durable in
// TaskMeta: submit_task writes submission_id plus a pending continuation with
// a deterministic delivery_id, but nothing re-wakes the Leader if the
// Worker's follow-up completion message is lost (crash between persistence
// and message, context compaction, offline leader, and so on). This package
// closes that gap on the Controller side: the elected Controller scans object
// storage for tasks whose continuation is still pending, and re-emits the
// wake-up through the Matrix channel.
//
// Boundary (design doc "PR1/PR2" split): this package only ever *re-wakes*
// the Leader. It never accepts a result, never resolves a continuation, never
// decides task status, and never redefines status mapping. Eligibility,
// fencing and idempotency stay in TeamHarness; the recovery path preserves
// the explicit check_task -> accept_task_result review boundary.
package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
)

// ErrTaskDecided is returned by a Notifier when a submission has no one left
// to wake: the project node is terminal, or the task is not yet addressable
// (no assignment room). The scanner must NOT record a sent wake for it.
var ErrTaskDecided = errors.New("recovery: task already decided or unroutable")

// Scanner detects TeamHarness tasks whose submission is still pending and
// dispatches a wake-up through the Notifier. It reads only object storage
// (no Kubernetes state), so its core logic is directly testable with an
// in-memory OSS fake.
type Scanner struct {
	Storager Storager
	Notifier Notifier
	// Logger is optional; nil disables scanner logging.
	Logger logr.Logger
	// MinInterval is the floor between two wake-ups for the same task
	// submission. Delivery is at-least-once: after a Controller restart the
	// scanner may re-wake a task whose earlier wake the Leader saw, and that
	// is acceptable because the Leader's accept path is fenced by
	// submission_id — a duplicate wake cannot produce a duplicate
	// acceptance.
	MinInterval time.Duration
}

// Storager is the subset of oss.StorageClient the scanner needs.
type Storager interface {
	GetObject(ctx context.Context, key string) ([]byte, error)
	PutObject(ctx context.Context, key string, data []byte) error
	// ListObjects returns the child names under a prefix. Real backends
	// (mc ls) report directory names without the prefix ("t-1/"); in-memory
	// fakes report full object keys ("shared/tasks/t-1/meta.json"). The
	// scanner accepts both shapes.
	ListObjects(ctx context.Context, prefix string) ([]string, error)
}

// Notifier dispatches one recovery wake-up for a still-pending submission.
// Implementations resolve the Leader identity (project -> team -> leader
// Matrix user) and send through Matrix; the Scanner never decides who the
// Leader is.
type Notifier interface {
	SendRecoveryWake(ctx context.Context, wake WakeMessage) error
}

// WakeMessage is one recovery wake-up emitted for a still-pending submission.
type WakeMessage struct {
	ProjectID string
	TaskID    string
	// TeamPrefix is the storage scope of the task: empty for the global
	// prefix (shared/), otherwise "teams/{name}/shared/". The recipient
	// resolves the owning project meta key from ProjectID + TeamPrefix.
	TeamPrefix   string
	RoomID       string
	SubmissionID string
	DeliveryID   string
	ResultPath   string
	// AssignedTo is the Worker identifier recorded in TaskMeta. It is NOT
	// the Leader; the recipient must resolve the Leader from the project's
	// team before sending.
	AssignedTo string
}

// Candidate is the subset of TaskMeta the scanner inspects.
type Candidate struct {
	ProjectID    string `json:"project_id,omitempty"`
	TaskID       string `json:"task_id,omitempty"`
	Status       string `json:"status,omitempty"`
	ResultPath   string `json:"result_path,omitempty"`
	RoomID       string `json:"room_id,omitempty"`
	AssignedTo   string `json:"assigned_to,omitempty"`
	SubmissionID string `json:"submission_id,omitempty"`
	Continuation struct {
		Status     string `json:"status,omitempty"`
		DeliveryID string `json:"delivery_id,omitempty"`
	} `json:"continuation,omitempty"`
}

// IsCandidate applies the design-doc eligibility conditions verbatim:
//
//	TaskMeta.status == submitted
//	AND continuation.status == pending
//	AND submission_id != ""
//	AND delivery_id != ""
//	AND corresponding project/task node is not terminal
//
// The terminal-node condition is enforced by the Notifier (which reads the
// project meta) — the scanner itself never lowers eligibility.
func IsCandidate(c Candidate) bool {
	if c.Status != "submitted" {
		return false
	}
	if c.Continuation.Status != "pending" {
		return false
	}
	if c.SubmissionID == "" || c.Continuation.DeliveryID == "" {
		return false
	}
	return true
}

// ScanResult reports what one scan pass did.
type ScanResult struct {
	TaskNodesScanned int
	CandidatesFound  int
	WakeSent         int
	WakeSkipped      int
	Errors           []error
}

// DefaultWakeInterval bounds repeated Matrix chatter for the same
// still-pending submission. It is not a delivery guarantee: the leader
// accept path's submission_id fence is the real idempotency boundary.
const DefaultWakeInterval = 15 * time.Minute

// ScanOnce performs one full scan pass and returns the outcome. Used by tests
// and by ScanLoop.
func (s *Scanner) ScanOnce(ctx context.Context) ScanResult {
	keys, err := s.DiscoverTaskMetaKeys(ctx)
	if err != nil {
		return ScanResult{Errors: []error{fmt.Errorf("discover task dirs: %w", err)}}
	}
	return s.scanKeys(ctx, keys, time.Now())
}

func (s *Scanner) scanKeys(ctx context.Context, metaKeys []string, now time.Time) ScanResult {
	res := ScanResult{}
	for _, metaKey := range metaKeys {
		candidate, ok := s.readTaskMeta(ctx, metaKey)
		if !ok {
			continue
		}
		res.TaskNodesScanned++
		if !IsCandidate(candidate) {
			continue
		}
		res.CandidatesFound++
		sent, err := s.wake(ctx, metaKey, candidate, now)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("task %s: %w", candidate.TaskID, err))
			continue
		}
		if sent {
			res.WakeSent++
		} else {
			res.WakeSkipped++
		}
	}
	return res
}

// DiscoverTaskMetaKeys lists every TaskMeta object key under the global
// prefix (shared/tasks/) and every team-scoped prefix
// (teams/{name}/shared/tasks/). A missing prefix is not an error.
func (s *Scanner) DiscoverTaskMetaKeys(ctx context.Context) ([]string, error) {
	var keys []string
	global, err := s.listTaskMetaKeys(ctx, "shared/tasks/")
	if err != nil {
		return nil, err
	}
	keys = append(keys, global...)

	// Team-scoped task dirs live under teams/{name}/shared/tasks/. The
	// backend reports either full keys ("teams/biz/...") or relative child
	// names ("biz/") depending on semantics; reduce both shapes to one
	// unique team prefix.
	teamChildren, err := s.Storager.ListObjects(ctx, "teams/")
	if err != nil {
		return nil, err
	}
	teamPrefixes := make(map[string]struct{})
	for _, teamChild := range teamChildren {
		teamChild = strings.TrimPrefix(teamChild, "teams/")
		teamName := strings.SplitN(teamChild, "/", 2)[0]
		if teamName == "" {
			continue
		}
		teamPrefixes["teams/"+teamName+"/"] = struct{}{}
	}
	for teamPrefix := range teamPrefixes {
		scoped, err := s.listTaskMetaKeys(ctx, teamPrefix+"shared/tasks/")
		if err != nil {
			return nil, err
		}
		keys = append(keys, scoped...)
	}
	return keys, nil
}

// listTaskMetaKeys lists meta.json object keys under one prefix. Backends
// report either child directory names ("t-1/", mc ls) or full object keys;
// both shapes are accepted. Directory names are relativized against prefix so
// the returned keys are always full object paths.
func (s *Scanner) listTaskMetaKeys(ctx context.Context, prefix string) ([]string, error) {
	children, err := s.Storager.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, child := range children {
		// mc ls reports direct children ("t-1/"); in-memory fakes and some
		// backends report full object keys ("shared/tasks/t-1/meta.json").
		child = strings.TrimPrefix(child, prefix)
		if child == "" {
			continue
		}
		switch {
		case strings.HasSuffix(child, "/"):
			keys = append(keys, prefix+child+"meta.json")
		case strings.HasSuffix(child, "meta.json"):
			keys = append(keys, prefix+child)
		}
	}
	return keys, nil
}

// readTaskMeta loads one TaskMeta object and applies the same ownership
// preconditions the project read path uses: task_id and project_id must be
// present, and task_id must match the directory the object lives in.
func (s *Scanner) readTaskMeta(ctx context.Context, key string) (Candidate, bool) {
	data, err := s.Storager.GetObject(ctx, key)
	if err != nil {
		// Missing object is the common case. A storage error must not abort
		// the pass.
		s.log().V(1).Info("recovery: skip unreadable task meta", "key", key, "error", err)
		return Candidate{}, false
	}
	var c Candidate
	if err := json.Unmarshal(data, &c); err != nil {
		s.log().V(1).Info("recovery: skip malformed task meta", "key", key, "error", err)
		return Candidate{}, false
	}
	if c.TaskID == "" || c.ProjectID == "" {
		return Candidate{}, false
	}
	if !strings.HasSuffix(key, "/"+c.TaskID+"/meta.json") {
		return Candidate{}, false
	}
	return c, true
}

// wake delivers one recovery wake-up, deduplicated by submission_id within
// MinInterval. It returns (true, nil) when a wake was sent, (false, nil)
// when the quiet window suppressed it, and (_, err) on delivery failure.
// The delivery record is persisted in object storage under the
// controller-owned controller/ prefix so a Controller restart does not flood
// the Leader for the same submission; it is a best-effort optimization, not a
// guarantee — the Leader-side submission_id fence stays the real boundary.
func (s *Scanner) wake(ctx context.Context, metaKey string, c Candidate, now time.Time) (bool, error) {
	interval := s.MinInterval
	if interval <= 0 {
		interval = DefaultWakeInterval
	}
	recordKey := wakeRecordPrefix + c.ProjectID + "/" + c.TaskID + "/" + c.SubmissionID + ".json"

	var rec wakeRecord
	if data, err := s.Storager.GetObject(ctx, recordKey); err == nil {
		_ = json.Unmarshal(data, &rec)
		if !rec.SentAt.IsZero() && now.Sub(rec.SentAt) < interval {
			return false, nil // within the quiet window
		}
	}

	wake := WakeMessage{
		ProjectID:    c.ProjectID,
		TaskID:       c.TaskID,
		TeamPrefix:   teamPrefixForTask(metaKey),
		RoomID:       c.RoomID,
		SubmissionID: c.SubmissionID,
		DeliveryID:   c.Continuation.DeliveryID,
		ResultPath:   c.ResultPath,
		AssignedTo:   c.AssignedTo,
	}
	if err := s.Notifier.SendRecoveryWake(ctx, wake); err != nil {
		if !errors.Is(err, ErrTaskDecided) {
			return false, err
		}
		// A decided/unroutable task is not a delivery failure: do not write
		// the wake record (the submission is done or not yet addressable),
		// and do not count it as sent. The next scan re-evaluates.
		return false, nil
	}

	rec = wakeRecord{
		ProjectID:    wake.ProjectID,
		TaskID:       wake.TaskID,
		SubmissionID: wake.SubmissionID,
		DeliveryID:   wake.DeliveryID,
		SentAt:       now.UTC(),
	}
	if data, err := json.Marshal(rec); err == nil {
		_ = s.Storager.PutObject(ctx, recordKey, data)
	}
	return true, nil
}

// wakeRecordPrefix is where the controller records emitted recovery wakes.
// It is deliberately NOT under TaskMeta: the controller owns this bookkeeping
// and TeamHarness keeps owning task state.
const wakeRecordPrefix = "controller/recovery-wake/"

type wakeRecord struct {
	ProjectID    string    `json:"project_id"`
	TaskID       string    `json:"task_id"`
	SubmissionID string    `json:"submission_id"`
	DeliveryID   string    `json:"delivery_id"`
	SentAt       time.Time `json:"sent_at"`
}

// teamPrefixForTask returns the storage scope of a task meta key: "" for the
// global prefix (shared/tasks/...), otherwise "teams/{name}/shared/".
func teamPrefixForTask(taskMetaKey string) string {
	if strings.HasPrefix(taskMetaKey, "shared/") {
		return ""
	}
	if strings.HasPrefix(taskMetaKey, "teams/") {
		if idx := strings.Index(taskMetaKey, "/shared/tasks/"); idx > 0 {
			return taskMetaKey[:idx+len("/shared/")]
		}
	}
	return ""
}

// Run drives the scan loop on the elected leader. It scans immediately, then
// on every interval, and blocks until ctx is cancelled. The interval is passed
// by the caller so config owns the cadence.
func (s *Scanner) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if result := s.ScanOnce(ctx); len(result.Errors) > 0 {
			s.log().V(1).Info("recovery scan finished with errors",
				"scanned", result.TaskNodesScanned,
				"candidates", result.CandidatesFound,
				"sent", result.WakeSent,
				"errors", len(result.Errors),
			)
			for _, e := range result.Errors {
				s.log().Error(e, "recovery wake error")
			}
		} else {
			s.log().Info("recovery scan complete",
				"scanned", result.TaskNodesScanned,
				"candidates", result.CandidatesFound,
				"sent", result.WakeSent,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Scanner) log() logr.Logger {
	return s.Logger
}

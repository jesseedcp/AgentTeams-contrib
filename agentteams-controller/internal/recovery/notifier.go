package recovery

import (
	"strings"
)

// CompletionBody builds the Leader-facing completion line for a recovery
// wake-up. It mirrors the task-execution skill contract Workers use so
// leader-side prompt parsing recognizes it as a completion signal. When the
// result path is unknown the canonical path is assumed (submit_task always
// writes result.md under shared/tasks/{task}/).
func CompletionBody(leaderID string, wake WakeMessage) string {
	leader := strings.TrimSpace(leaderID)
	path := strings.TrimSpace(wake.ResultPath)
	if path == "" {
		path = "shared/tasks/" + wake.TaskID + "/result.md"
	}
	return leader + " TASK_COMPLETED: " + wake.TaskID + " - Result: " + path +
		"\nProject: " + wake.ProjectID +
		"\nSubmission: " + wake.SubmissionID +
		"\nDelivery: " + wake.DeliveryID
}

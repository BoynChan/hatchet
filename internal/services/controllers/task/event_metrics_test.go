package task

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestCompletionMetricWorker(t *testing.T) {
	worker, other := uuid.New(), uuid.New()
	for _, tt := range []struct {
		name     string
		row      *sqlcv1.ReleaseTasksRow
		reported uuid.UUID
		want     uuid.UUID
	}{
		{"normal_runtime_wins", &sqlcv1.ReleaseTasksRow{HadRuntime: true, IsCurrentRetry: true, WorkerID: worker}, other, worker},
		{"early_slot_release", &sqlcv1.ReleaseTasksRow{HadRuntime: true, IsCurrentRetry: true}, worker, worker},
		{"redelivery", &sqlcv1.ReleaseTasksRow{IsCurrentRetry: true}, worker, uuid.Nil},
		{"stale_retry", &sqlcv1.ReleaseTasksRow{HadRuntime: true, WorkerID: worker}, worker, uuid.Nil},
		{"orchestrator", &sqlcv1.ReleaseTasksRow{HadRuntime: true, IsCurrentRetry: true, IsDagOrchestrator: true}, worker, uuid.Nil},
		{"old_message_without_worker", &sqlcv1.ReleaseTasksRow{HadRuntime: true, IsCurrentRetry: true}, uuid.Nil, uuid.Nil},
	} {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, completionMetricWorker(tt.row, tt.reported)) })
	}
}

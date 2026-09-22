package task

import (
	"github.com/google/uuid"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type completionAttempt struct {
	id               int64
	insertedAtMicros int64
	retry            int32
}

func completionMetricWorker(released *sqlcv1.ReleaseTasksRow, reportedWorker uuid.UUID) uuid.UUID {
	// Runtime presence distinguishes an early slot release from a redelivered
	// completion. Both can have a nil runtime worker, but only the former counts.
	if released == nil || !released.HadRuntime || !released.IsCurrentRetry || released.IsDagOrchestrator {
		return uuid.Nil
	}
	if released.WorkerID != uuid.Nil {
		return released.WorkerID
	}
	return reportedWorker
}

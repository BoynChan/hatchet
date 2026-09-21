//go:build !e2e && !load && !rampup && !integration

package v1

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestReleaseRefreshWaitsForStaleReadThenLoadsCapacity(t *testing.T) {
	tenantID, workerID := uuid.New(), uuid.New()
	readStarted, finishRead := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var reads atomic.Int32
	s := newTestScheduler(t, tenantID, &mockAssignmentRepo{
		listActionsForWorkersFn: func(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.ListActionsForWorkersRow, error) {
			return []*sqlcv1.ListActionsForWorkersRow{{WorkerId: workerID, ActionId: pgtype.Text{String: "A", Valid: true}}}, nil
		},
		listAvailableSlotsForWorkersFn: func(ctx context.Context, _ uuid.UUID, _ sqlcv1.ListAvailableSlotsForWorkersParams) ([]*sqlcv1.ListAvailableSlotsForWorkersRow, error) {
			available := int32(1)
			if reads.Add(1) == 1 {
				available = 0
				close(readStarted)
				select {
				case <-finishRead:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return []*sqlcv1.ListAvailableSlotsForWorkersRow{{ID: workerID, AvailableSlots: available}}, nil
		},
	})
	onLoop(t, s, func() { s.workers[workerID] = &worker{ListActiveWorkersResult: testWorker(workerID)} })
	first := make(chan error, 1)
	go func() { first <- s.replenish(ctx, true) }()
	select {
	case <-readStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A cancelled release notification must not wait indefinitely for database I/O.
	cancelled, stop := context.WithCancel(ctx)
	stop()
	require.ErrorIs(t, s.replenishAfterRelease(cancelled), context.Canceled)
	second := make(chan error, 1)
	go func() { second <- s.replenishAfterRelease(ctx) }()
	select {
	case err := <-second:
		t.Fatalf("refresh returned before stale read completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(finishRead)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.Equal(t, int32(2), reads.Load(), "release needs a read started after its notification")
	assigned, err := s.tryAssignBatch(ctx, "A", []*sqlcv1.V1QueueItem{testQI(tenantID, "A", 1)}, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.Len(t, assigned, 1)
	require.True(t, assigned[0].succeeded, "released capacity must be assignable without waiting for the poll timer")
}

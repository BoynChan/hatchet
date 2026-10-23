//go:build !e2e && !load && !rampup && !integration

package v1

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	repo "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
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

// enteredReleaseContext lets a burst start before the blocked database read is
// allowed to finish, without depending on goroutine scheduling or sleeps.
type enteredReleaseContext struct {
	context.Context
	once    sync.Once
	entered chan<- struct{}
}

func (c *enteredReleaseContext) Err() error {
	c.once.Do(func() { c.entered <- struct{}{} })
	return c.Context.Err()
}

func TestReleaseRefreshCoalescesBurstAfterStaleRead(t *testing.T) {
	for _, failSharedRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared_read_failure_%t", failSharedRead), func(t *testing.T) {
			tenantID, workerID := uuid.New(), uuid.New()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			readStarted, finishRead := make(chan struct{}), make(chan struct{})
			var actionReads, configReads, capacityReads atomic.Int32
			readErr := errors.New("capacity read failed")
			s := newTestScheduler(t, tenantID, &mockAssignmentRepo{
				listActionsForWorkersFn: func(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.ListActionsForWorkersRow, error) {
					actionReads.Add(1)
					return []*sqlcv1.ListActionsForWorkersRow{{WorkerId: workerID, ActionId: pgtype.Text{String: "A", Valid: true}}}, nil
				},
				listWorkerSlotConfigsFn: func(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.ListWorkerSlotConfigsRow, error) {
					configReads.Add(1)
					return []*sqlcv1.ListWorkerSlotConfigsRow{{WorkerID: workerID, SlotType: repo.SlotTypeDefault, MaxUnits: 1}}, nil
				},
				listAvailableSlotsForWorkersFn: func(ctx context.Context, _ uuid.UUID, _ sqlcv1.ListAvailableSlotsForWorkersParams) ([]*sqlcv1.ListAvailableSlotsForWorkersRow, error) {
					n := capacityReads.Add(1)
					if n == 1 {
						close(readStarted)
						select {
						case <-finishRead:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
						return []*sqlcv1.ListAvailableSlotsForWorkersRow{{ID: workerID, AvailableSlots: 0}}, nil
					}
					if n == 2 && failSharedRead {
						return nil, readErr
					}
					return []*sqlcv1.ListAvailableSlotsForWorkersRow{{ID: workerID, AvailableSlots: 1}}, nil
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

			const burst = 32
			entered, results := make(chan struct{}, burst), make(chan error, burst)
			for range burst {
				go func() {
					results <- s.replenishAfterRelease(&enteredReleaseContext{Context: ctx, entered: entered})
				}()
			}
			for range burst {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			close(finishRead)
			require.NoError(t, <-first)
			failures := 0
			for range burst {
				select {
				case err := <-results:
					if err != nil {
						require.ErrorIs(t, err, readErr)
						failures++
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			wantReads := int32(2)
			if failSharedRead {
				wantReads++
				require.Equal(t, 1, failures, "a failed shared read cannot satisfy other releases")
			} else {
				require.Zero(t, failures)
			}
			require.Equal(t, wantReads, actionReads.Load())
			require.Equal(t, wantReads, configReads.Load())
			require.Equal(t, wantReads, capacityReads.Load())
			assigned, err := s.tryAssignBatch(ctx, "A", []*sqlcv1.V1QueueItem{testQI(tenantID, "A", 1)}, nil, nil, nil, nil, nil)
			require.NoError(t, err)
			require.Len(t, assigned, 1)
			require.True(t, assigned[0].succeeded)
		})
	}
}

func TestReleaseRefreshPacesReadsAndHonorsCancellation(t *testing.T) {
	tenantID, workerID := uuid.New(), uuid.New()
	var readTimes []time.Time
	s := newTestScheduler(t, tenantID, &mockAssignmentRepo{
		listActionsForWorkersFn: func(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.ListActionsForWorkersRow, error) {
			return []*sqlcv1.ListActionsForWorkersRow{{WorkerId: workerID, ActionId: pgtype.Text{String: "A", Valid: true}}}, nil
		},
		listAvailableSlotsForWorkersFn: func(context.Context, uuid.UUID, sqlcv1.ListAvailableSlotsForWorkersParams) ([]*sqlcv1.ListAvailableSlotsForWorkersRow, error) {
			readTimes = append(readTimes, time.Now())
			return []*sqlcv1.ListAvailableSlotsForWorkersRow{{ID: workerID, AvailableSlots: 1}}, nil
		},
	})
	onLoop(t, s, func() { s.workers[workerID] = &worker{ListActiveWorkersResult: testWorker(workerID)} })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.replenishAfterRelease(ctx))
	shortCtx, stop := context.WithTimeout(ctx, 5*time.Millisecond)
	defer stop()
	require.ErrorIs(t, s.replenishAfterRelease(shortCtx), context.DeadlineExceeded)
	require.Len(t, readTimes, 1, "cancelled pacing wait must not query the database")
	for range 2 {
		require.NoError(t, s.replenishAfterRelease(ctx))
	}
	require.Len(t, readTimes, 3)
	for i := 1; i < len(readTimes); i++ {
		require.GreaterOrEqual(t, readTimes[i].Sub(readTimes[i-1]), 45*time.Millisecond)
	}
}

func TestReleaseArrivingDuringSharedReadRequiresNextRead(t *testing.T) {
	tenantID, workerID := uuid.New(), uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	finish := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	var reads atomic.Int32
	s := newTestScheduler(t, tenantID, &mockAssignmentRepo{
		listActionsForWorkersFn: func(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.ListActionsForWorkersRow, error) {
			return []*sqlcv1.ListActionsForWorkersRow{{WorkerId: workerID, ActionId: pgtype.Text{String: "A", Valid: true}}}, nil
		},
		listAvailableSlotsForWorkersFn: func(ctx context.Context, _ uuid.UUID, _ sqlcv1.ListAvailableSlotsForWorkersParams) ([]*sqlcv1.ListAvailableSlotsForWorkersRow, error) {
			n := reads.Add(1)
			if n <= int32(len(started)) {
				close(started[n-1])
				select {
				case <-finish[n-1]:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return []*sqlcv1.ListAvailableSlotsForWorkersRow{{ID: workerID, AvailableSlots: n - 1}}, nil
		},
	})
	onLoop(t, s, func() { s.workers[workerID] = &worker{ListActiveWorkersResult: testWorker(workerID)} })
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	initial := make(chan error, 1)
	go func() { initial <- s.replenish(ctx, true) }()
	wait(started[0])
	entered := make(chan struct{}, 1)
	early := make(chan error, 1)
	go func() { early <- s.replenishAfterRelease(&enteredReleaseContext{Context: ctx, entered: entered}) }()
	wait(entered)
	close(finish[0])
	require.NoError(t, <-initial)
	wait(started[1])
	late := make(chan error, 1)
	go func() { late <- s.replenishAfterRelease(&enteredReleaseContext{Context: ctx, entered: entered}) }()
	wait(entered)
	close(finish[1])
	require.NoError(t, <-early)
	wait(started[2])
	select {
	case err := <-late:
		t.Fatalf("late release returned before its fresh read completed: %v", err)
	default:
	}
	close(finish[2])
	require.NoError(t, <-late)
	require.Equal(t, int32(3), reads.Load())
}

func TestReleaseRefreshCoalescesWithoutRegisteredActions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, finish := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	s := newTestScheduler(t, uuid.New(), &mockAssignmentRepo{
		listActionsForWorkersFn: func(ctx context.Context, _ uuid.UUID, _ []uuid.UUID) ([]*sqlcv1.ListActionsForWorkersRow, error) {
			if reads.Add(1) == 1 {
				close(started)
				select {
				case <-finish:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		},
	})
	initial := make(chan error, 1)
	go func() { initial <- s.replenish(ctx, true) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	entered, results := make(chan struct{}, 2), make(chan error, 2)
	for range 2 {
		go func() { results <- s.replenishAfterRelease(&enteredReleaseContext{Context: ctx, entered: entered}) }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(finish)
	require.NoError(t, <-initial)
	for range 2 {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.Equal(t, int32(2), reads.Load())
}

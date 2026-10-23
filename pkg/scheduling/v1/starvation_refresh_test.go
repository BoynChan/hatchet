//go:build !e2e && !load && !rampup && !integration

package v1

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	repo "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/stretchr/testify/require"
)

func TestScheduler_Replenish_BusyWorkersDoNotRepeatHeuristicRefresh(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(fmt.Sprintf("release_%t", release), func(t *testing.T) {
			testBusyWorkerRefresh(t, release)
		})
	}
}

func testBusyWorkerRefresh(t *testing.T, release bool) {
	t.Helper()
	tenantID, workerID := uuid.New(), uuid.New()
	available := map[uuid.UUID]map[string]int32{workerID: {repo.SlotTypeDefault: 0}}
	ar := starvedActionRepo(available)
	actionReads, configReads, capacityReads := 0, 0, 0
	readActions := ar.listActionsForWorkersFn
	ar.listActionsForWorkersFn = func(ctx context.Context, tenant uuid.UUID, workers []uuid.UUID) ([]*sqlcv1.ListActionsForWorkersRow, error) {
		actionReads++
		return readActions(ctx, tenant, workers)
	}
	ar.listWorkerSlotConfigsFn = func(context.Context, uuid.UUID, []uuid.UUID) ([]*sqlcv1.ListWorkerSlotConfigsRow, error) {
		configReads++
		return []*sqlcv1.ListWorkerSlotConfigsRow{{WorkerID: workerID, SlotType: repo.SlotTypeDefault, MaxUnits: 1}}, nil
	}
	readAvailable := ar.listAvailableSlotsForWorkersAndTypesFn
	ar.listAvailableSlotsForWorkersAndTypesFn = func(ctx context.Context, tenant uuid.UUID, params sqlcv1.ListAvailableSlotsForWorkersAndTypesParams) ([]*sqlcv1.ListAvailableSlotsForWorkersAndTypesRow, error) {
		capacityReads++
		return readAvailable(ctx, tenant, params)
	}
	s := newTestScheduler(t, tenantID, ar)
	var restored []string
	s.onCapacityRestored = func(queues []string) { restored = append(restored, queues...) }
	s.setWorkers([]*repo.ListActiveWorkersResult{testWorker(workerID)})
	require.NoError(t, s.replenish(context.Background(), true))
	qi := testQI(tenantID, "A", 1)
	assign := func() *assignSingleResult {
		results, err := s.tryAssignBatch(context.Background(), "A", []*sqlcv1.V1QueueItem{qi}, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.Len(t, results, 1)
		return results[0]
	}
	for round := 0; round < 100; round++ {
		require.True(t, assign().noSlots)
		select {
		case <-s.notifyReplenishCh:
			require.NoError(t, s.replenish(context.Background(), false))
		default:
		}
	}
	t.Logf("reads for 100 retries with unchanged zero capacity: actions=%d configs=%d capacity=%d", actionReads, configReads, capacityReads)
	require.Equal(t, 2, actionReads, "only initialization and the first miss request reads")
	require.Equal(t, 2, configReads)
	require.Equal(t, 2, capacityReads)
	require.Empty(t, restored, "an empty refresh cannot wake the queue")
	onLoop(t, s, func() {
		require.True(t, s.actions["A"].IsStarved())
		require.Equal(t, []uuid.UUID{workerID}, s.actions["A"].workerIds)
	})

	// Both periodic refreshes and release notifications must restore capacity
	// without requiring another assignment miss to request a read.
	available[workerID][repo.SlotTypeDefault] = 1
	if release {
		require.NoError(t, s.replenishAfterRelease(context.Background()))
	} else {
		require.NoError(t, s.replenish(context.Background(), true))
	}
	require.Equal(t, 3, capacityReads)
	require.Equal(t, []string{qi.Queue}, restored)
	onLoop(t, s, func() { require.False(t, s.actions["A"].IsStarved()) })
	require.True(t, assign().succeeded)
	qi = testQI(tenantID, "A", 2)
	require.True(t, assign().noSlots, "the recovered slot can only be assigned once")
	require.Len(t, s.notifyReplenishCh, 1, "a new starvation episode still requests a refresh")
}

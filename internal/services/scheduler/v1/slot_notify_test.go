package scheduler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	tasktypes "github.com/hatchet-dev/hatchet/internal/services/shared/tasktypes/v1"
	"github.com/hatchet-dev/hatchet/pkg/scheduling"
)

type releaseOrderPool struct {
	scheduling.Pool
	t          *testing.T
	tenantID   uuid.UUID
	available  bool
	woke       bool
	refreshes  int
	queues     []string
	strategies []int64
}

func (p *releaseOrderPool) Replenish(_ context.Context, tenantID uuid.UUID) {
	require.Equal(p.t, p.tenantID, tenantID)
	p.available = true
	p.refreshes++
}

func (p *releaseOrderPool) NotifyQueues(_ context.Context, tenantID uuid.UUID, queues []string) {
	require.Equal(p.t, p.tenantID, tenantID)
	require.True(p.t, p.available, "assignment must observe released capacity when it wakes")
	p.woke = true
	p.queues = append(p.queues, queues...)
}

func (p *releaseOrderPool) NotifyConcurrency(_ context.Context, tenantID uuid.UUID, strategies []int64) {
	require.Equal(p.t, p.tenantID, tenantID)
	require.True(p.t, p.available, "concurrency notification can also start assignment")
	p.strategies = append(p.strategies, strategies...)
}

func TestSlotReleaseBatchRefreshesOnceBeforeAllWakeups(t *testing.T) {
	p := &releaseOrderPool{t: t, tenantID: uuid.New()}
	s := &Scheduler{pool: p}
	msg, err := msgqueue.NewTenantMessage(p.tenantID, msgqueue.MsgIDCheckTenantQueue, true, false,
		tasktypes.CheckTenantQueuesPayload{QueueNames: []string{"first-queue"}, StrategyIds: []int64{1}},
		tasktypes.CheckTenantQueuesPayload{SlotsReleased: true, QueueNames: []string{"second-queue"}},
		tasktypes.CheckTenantQueuesPayload{SlotsReleased: true, StrategyIds: []int64{2}},
		tasktypes.CheckTenantQueuesPayload{SlotsReleased: true, QueueNames: []string{"third-queue"}},
	)
	require.NoError(t, err)
	require.NoError(t, s.handleCheckQueue(context.Background(), msg))
	require.Equal(t, 1, p.refreshes)
	require.Equal(t, []string{"first-queue", "second-queue", "third-queue"}, p.queues)
	require.Equal(t, []int64{1, 2}, p.strategies)
}

func TestQueueNotificationWithoutReleaseDoesNotRefresh(t *testing.T) {
	p := &releaseOrderPool{t: t, tenantID: uuid.New(), available: true}
	s := &Scheduler{pool: p}
	msg, err := msgqueue.NewTenantMessage(p.tenantID, msgqueue.MsgIDCheckTenantQueue, true, false,
		tasktypes.CheckTenantQueuesPayload{QueueNames: []string{"first-queue"}, StrategyIds: []int64{1}})
	require.NoError(t, err)
	require.NoError(t, s.handleCheckQueue(context.Background(), msg))
	require.Zero(t, p.refreshes)
	require.Equal(t, []string{"first-queue"}, p.queues)
	require.Equal(t, []int64{1}, p.strategies)
}

func TestSlotReleaseRefreshesCapacityBeforeWakingQueues(t *testing.T) {
	p := &releaseOrderPool{t: t, tenantID: uuid.New()}
	s := &Scheduler{pool: p}
	msg, err := msgqueue.NewTenantMessage(p.tenantID, msgqueue.MsgIDCheckTenantQueue, true, false,
		tasktypes.CheckTenantQueuesPayload{SlotsReleased: true, QueueNames: []string{"upload-queue"}, StrategyIds: []int64{1}})
	require.NoError(t, err)
	require.NoError(t, s.handleCheckQueue(context.Background(), msg))
	require.True(t, p.woke)
}

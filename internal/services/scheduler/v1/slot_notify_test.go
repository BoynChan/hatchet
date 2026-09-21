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
	t         *testing.T
	tenantID  uuid.UUID
	available bool
	woke      bool
}

func (p *releaseOrderPool) Replenish(_ context.Context, tenantID uuid.UUID) {
	require.Equal(p.t, p.tenantID, tenantID)
	p.available = true
}

func (p *releaseOrderPool) NotifyQueues(_ context.Context, tenantID uuid.UUID, _ []string) {
	require.Equal(p.t, p.tenantID, tenantID)
	require.True(p.t, p.available, "assignment must observe released capacity when it wakes")
	p.woke = true
}

func (p *releaseOrderPool) NotifyConcurrency(_ context.Context, tenantID uuid.UUID, _ []int64) {
	require.Equal(p.t, p.tenantID, tenantID)
	require.True(p.t, p.available, "concurrency notification can also start assignment")
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

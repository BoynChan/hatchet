package dispatcher

import (
	"context"
	"time"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	tasktypes "github.com/hatchet-dev/hatchet/internal/services/shared/tasktypes/v1"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func (d *DispatcherImpl) notifySlotReleased(ctx context.Context, tenant *sqlcv1.Tenant, queue string) {
	if !tenant.SchedulerPartitionId.Valid {
		return
	}

	// A committed release must still wake scheduling if its RPC is cancelled.
	// Notification failure must not turn a successful release into a task failure;
	// periodic capacity refresh and queue polling remain the recovery path.
	notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()

	msg, err := msgqueue.NewTenantMessage(tenant.ID, msgqueue.MsgIDCheckTenantQueue, true, false,
		tasktypes.CheckTenantQueuesPayload{
			SlotsReleased: true,
			QueueNames:    []string{queue},
			// Workflow concurrency is held until the task actually completes.
		})
	if err == nil {
		err = d.pubsub.Pub(notifyCtx, msgqueue.SchedulerPartitionTopic(tenant.SchedulerPartitionId.String), msg)
	}
	if err != nil {
		d.l.Error().Err(err).Ctx(ctx).Str("scheduler_partition_id", tenant.SchedulerPartitionId.String).
			Msg("could not notify scheduler of manual slot release")
	}
}

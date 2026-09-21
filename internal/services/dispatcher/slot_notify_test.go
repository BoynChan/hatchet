package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	tasktypes "github.com/hatchet-dev/hatchet/internal/services/shared/tasktypes/v1"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type slotReleasePubSub struct {
	msgqueue.PubSub
	pub func(context.Context, msgqueue.Topic, *msgqueue.Message) error
}

func (p *slotReleasePubSub) Pub(ctx context.Context, topic msgqueue.Topic, msg *msgqueue.Message) error {
	return p.pub(ctx, topic, msg)
}

func TestNotifySlotReleased(t *testing.T) {
	for _, publishError := range []error{nil, errors.New("notification unavailable")} {
		t.Run(fmtErrorName(publishError), func(t *testing.T) {
			tenant := &sqlcv1.Tenant{ID: uuid.New(), SchedulerPartitionId: pgtype.Text{String: "partition-a", Valid: true}}
			called := false
			ps := &slotReleasePubSub{pub: func(ctx context.Context, topic msgqueue.Topic, msg *msgqueue.Message) error {
				called = true
				require.NoError(t, ctx.Err())
				_, bounded := ctx.Deadline()
				require.True(t, bounded)
				require.Equal(t, msgqueue.SchedulerPartitionTopic("partition-a"), topic)
				require.Equal(t, tenant.ID, msg.TenantID)
				require.Equal(t, msgqueue.MsgIDCheckTenantQueue, msg.ID)
				require.Len(t, msg.Payloads, 1)
				var payload tasktypes.CheckTenantQueuesPayload
				require.NoError(t, json.Unmarshal(msg.Payloads[0], &payload))
				require.True(t, payload.SlotsReleased)
				require.Equal(t, []string{"upload-queue"}, payload.QueueNames)
				require.Empty(t, payload.StrategyIds, "manual release must not finalize workflow concurrency")
				return publishError
			}}
			d := &DispatcherImpl{l: zerologNop(), pubsub: ps}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			d.notifySlotReleased(ctx, tenant, "upload-queue")
			require.True(t, called)
			called = false
			tenant.SchedulerPartitionId.Valid = false
			d.notifySlotReleased(ctx, tenant, "upload-queue")
			require.False(t, called)
		})
	}
}

func fmtErrorName(err error) string {
	if err != nil {
		return "publication_failure"
	}
	return "publication_success"
}

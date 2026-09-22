package dispatcher

import (
	"context"
	"testing"

	"github.com/google/uuid"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestDispatchMetricsMissingWorkerAndCancelledContext(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing_worker", true: "cancelled_before_attempt"}[cancelled], func(t *testing.T) {
			tenant, worker := uuid.New(), uuid.New()
			log := zerolog.Nop()
			d := &DispatcherImpl{workers: &workers{}, l: &log}
			task := &sqlcv1.V1Task{ID: 1, TenantID: tenant, Queue: "queue", ExternalID: uuid.New(), RetryCount: 0}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelled {
				cancel()
			}
			requeued := 0
			err := d.sendTasksToWorker(ctx, func(*sqlcv1.V1Task) { requeued++ }, tenant, worker, []int64{1}, map[int64]*V1TaskWithPayloadAndInvocationCount{
				1: {V1TaskWithPayload: &v1.V1TaskWithPayload{V1Task: task}},
			})
			require.NoError(t, err)
			require.Equal(t, 1, requeued)
			families, err := promclient.DefaultGatherer.Gather()
			require.NoError(t, err)
			var attempts float64
			for _, family := range families {
				if family.GetName() != "hatchet_task_dispatch_attempts_total" {
					continue
				}
				for _, metric := range family.Metric {
					for _, label := range metric.Label {
						if label.GetName() == "tenant_id" && label.GetValue() == tenant.String() {
							attempts += metric.GetCounter().GetValue()
						}
					}
				}
			}
			if cancelled {
				require.Zero(t, attempts)
			} else {
				require.Equal(t, 1.0, attempts)
			}
		})
	}
}

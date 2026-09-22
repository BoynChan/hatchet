package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// EventHooks records observations from committed lifecycle operations. It performs
// no I/O and deliberately has no task/run identifiers in its metric labels.
var EventHooks = newEventMetrics(prometheus.DefaultRegisterer)

type eventMetrics struct {
	assignmentDelay *prometheus.HistogramVec
	dispatches      *prometheus.CounterVec
	completions     *prometheus.CounterVec
}

func newEventMetrics(reg prometheus.Registerer) *eventMetrics {
	m := &eventMetrics{
		assignmentDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hatchet_task_initial_assignment_delay_seconds",
			Help:    "Time from task creation to committed assignment on retry zero, including optimistic scheduling; not a pure queue-residency duration.",
			Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.5, 1, 2, 5, 15, 30, 60, 120, 300, 600},
		}, []string{"tenant_id", "queue"}),
		dispatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hatchet_task_dispatch_attempts_total",
			Help: "Single-task dispatcher send outcomes, not worker acknowledgements; includes message redelivery, excludes batch-start RPCs.",
		}, []string{"tenant_id", "queue", "worker_id", "retry_bucket", "outcome"}),
		completions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hatchet_task_worker_completions_total",
			Help: "Committed completions releasing a current worker runtime, including tasks whose slots were released early.",
		}, []string{"tenant_id", "queue", "worker_id"}),
	}
	reg.MustRegister(m.assignmentDelay, m.dispatches, m.completions)
	return m
}

func (m *eventMetrics) RecordAssignment(tenant, queue string, retry int32, createdAt, assignedAt time.Time) {
	if retry != 0 || createdAt.IsZero() || assignedAt.Before(createdAt) {
		return
	}
	m.assignmentDelay.WithLabelValues(tenant, queue).Observe(assignedAt.Sub(createdAt).Seconds())
}

func dispatchRetryBucket(retry int32) string {
	switch retry {
	case 0:
		return "0"
	case 1:
		return "1"
	case 2:
		return "2"
	case 3:
		return "3"
	default:
		return "4+"
	}
}

func (m *eventMetrics) RecordDispatch(tenant, queue, worker string, retry int32, sent bool) {
	if retry < 0 {
		return
	}
	bucket := dispatchRetryBucket(retry)
	// Initialize both outcomes so a failure-free observed series has a zero numerator.
	success := m.dispatches.WithLabelValues(tenant, queue, worker, bucket, "sent")
	failure := m.dispatches.WithLabelValues(tenant, queue, worker, bucket, "failed")
	if sent {
		success.Inc()
	} else {
		failure.Inc()
	}
}

func (m *eventMetrics) RecordCompletion(tenant, queue, worker string, currentRetry bool) {
	// Redelivery after runtime deletion has no worker; stale attempts are not
	// completions of the current attempt. Early slot release retains the runtime.
	if !currentRetry || worker == "" {
		return
	}
	m.completions.WithLabelValues(tenant, queue, worker).Inc()
}

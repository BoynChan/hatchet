package prometheus

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestEventHooksAssignmentExcludesRetriesAndInvalidTimestamps(t *testing.T) {
	r := prometheus.NewRegistry()
	m := newEventMetrics(r)
	start := time.Unix(1000, 0)
	m.RecordAssignment("tenant", "queue", 0, start, start.Add(2*time.Second))
	m.RecordAssignment("tenant", "queue", 1, start, start.Add(20*time.Second))
	m.RecordAssignment("tenant", "queue", 0, time.Time{}, start)
	m.RecordAssignment("tenant", "queue", 0, start, start.Add(-time.Second))
	families, err := r.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	h := families[0].Metric[0].GetHistogram()
	require.EqualValues(t, 1, h.GetSampleCount())
	require.Equal(t, 2.0, h.GetSampleSum())
}

func TestEventHooksDispatchCountsAttemptsWithBoundedRetryLabels(t *testing.T) {
	r := prometheus.NewRegistry()
	m := newEventMetrics(r)
	m.RecordDispatch("tenant", "queue", "worker", 0, true)
	m.RecordDispatch("tenant", "queue", "worker", 0, false)
	m.RecordDispatch("tenant", "queue", "worker", 4, false)
	m.RecordDispatch("tenant", "queue", "worker", 10000, true)
	m.RecordDispatch("tenant", "queue", "worker", -1, true)
	require.Equal(t, 1.0, testutil.ToFloat64(m.dispatches.WithLabelValues("tenant", "queue", "worker", "0", "sent")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.dispatches.WithLabelValues("tenant", "queue", "worker", "0", "failed")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.dispatches.WithLabelValues("tenant", "queue", "worker", "4+", "sent")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.dispatches.WithLabelValues("tenant", "queue", "worker", "4+", "failed")))
	require.Equal(t, 4, testutil.CollectAndCount(m.dispatches))
}

func TestEventHooksFailureFreeDispatchExportsZeroFailures(t *testing.T) {
	m := newEventMetrics(prometheus.NewRegistry())
	m.RecordDispatch("tenant", "queue", "worker", 0, true)
	require.Equal(t, 2, testutil.CollectAndCount(m.dispatches))
	require.Zero(t, testutil.ToFloat64(m.dispatches.WithLabelValues("tenant", "queue", "worker", "0", "failed")))
}

func TestEventHooksCompletionIgnoresReleasedRuntimeRedeliveryAndStaleAttempts(t *testing.T) {
	m := newEventMetrics(prometheus.NewRegistry())
	// The committed release returns a worker even after an early slot release.
	m.RecordCompletion("tenant", "queue", "worker", true)
	// A redelivery after runtime deletion no longer has a worker to release.
	m.RecordCompletion("tenant", "queue", "", true)
	m.RecordCompletion("tenant", "queue", "worker", false)
	require.Equal(t, 1.0, testutil.ToFloat64(m.completions.WithLabelValues("tenant", "queue", "worker")))
	require.Equal(t, 1, testutil.CollectAndCount(m.completions))
}

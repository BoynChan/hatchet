package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	tasktypes "github.com/hatchet-dev/hatchet/internal/services/shared/tasktypes/v1"
	repov1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	v1 "github.com/hatchet-dev/hatchet/pkg/scheduling/v1"
)

type blockedEventQueue struct {
	msgqueue.MessageQueue
	started chan struct{}
	release chan struct{}
	once    sync.Once
	events  atomic.Int64
}

func (q *blockedEventQueue) SendMessage(ctx context.Context, _ msgqueue.Queue, msg *msgqueue.Message) error {
	q.once.Do(func() { close(q.started) })
	select {
	case <-q.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, event := range msgqueue.JSONConvert[tasktypes.CreateMonitoringEventPayload](msg.Payloads) {
		if event.TaskId == 42 {
			q.events.Add(1)
		}
	}
	return nil
}

func TestSchedulerDeduplicatesEventsWhilePublisherIsFull(t *testing.T) {
	for _, kind := range []string{"unassigned", "rate_limited"} {
		t.Run(kind, func(t *testing.T) {
			q := &blockedEventQueue{started: make(chan struct{}), release: make(chan struct{})}
			var unblock sync.Once
			release := func() { unblock.Do(func() { close(q.release) }) }
			t.Cleanup(release)
			buffer := msgqueue.NewMQPubBuffer(q)
			t.Cleanup(buffer.Stop)
			log := zerolog.Nop()
			s := &Scheduler{pubBuffer: buffer, l: &log,
				tasksWithNoWorkerCache: expirable.NewLRU[string, struct{}](10000, nil, 5*time.Minute),
				rateLimitedTasksCache:  expirable.NewLRU[string, struct{}](10000, nil, time.Minute)}
			tenantID := uuid.New()
			filler, err := tasktypes.MonitoringEventMessageFromInternal(tenantID, tasktypes.CreateMonitoringEventPayload{TaskId: 1})
			require.NoError(t, err)
			require.NoError(t, buffer.Pub(context.Background(), msgqueue.OLAP_QUEUE, filler, false))
			select {
			case <-q.started:
			case <-time.After(time.Second):
				t.Fatal("publisher did not start")
			}
			for i := 0; i < msgqueue.PUB_BUFFER_SIZE; i++ {
				require.NoError(t, buffer.Pub(context.Background(), msgqueue.OLAP_QUEUE, filler, false))
			}
			result := &v1.QueueResults{TenantId: tenantID}
			if kind == "unassigned" {
				result.Unassigned = []*sqlcv1.V1QueueItem{{TaskID: 42, ExternalID: uuid.New()}}
			} else {
				result.RateLimited = []*repov1.RateLimitResult{{TaskId: 42}}
			}
			const callers = 64
			done := make(chan error, callers)
			for i := 0; i < callers; i++ {
				go func() { done <- s.scheduleStepRuns(context.Background(), tenantID, result) }()
			}
			// Only one caller may wait for the full publisher. Duplicates must finish
			// while that first event is still pending, without retaining more results.
			finished := 0
			deadline := time.NewTimer(500 * time.Millisecond)
		waitDuplicates:
			for finished < callers-1 {
				select {
				case err := <-done:
					require.NoError(t, err)
					finished++
				case <-deadline.C:
					break waitDuplicates
				}
			}
			deadline.Stop()
			release()
			for i := finished; i < callers; i++ {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Fatal("scheduler did not drain")
				}
			}
			// A synchronous marker is behind every asynchronous event in this buffer.
			require.NoError(t, buffer.Pub(context.Background(), msgqueue.OLAP_QUEUE, filler, true))
			require.Equal(t, callers-1, finished, "duplicate handlers accumulated behind the blocked publisher")
			require.EqualValues(t, 1, q.events.Load(), "duplicate monitoring events were published")
		})
	}
}

func TestSchedulerRetriesMonitoringEventAfterCancelledEnqueue(t *testing.T) {
	q := &blockedEventQueue{started: make(chan struct{}), release: make(chan struct{})}
	close(q.release)
	buffer := msgqueue.NewMQPubBuffer(q)
	defer buffer.Stop()
	log := zerolog.Nop()
	s := &Scheduler{pubBuffer: buffer, l: &log,
		tasksWithNoWorkerCache: expirable.NewLRU[string, struct{}](10000, nil, time.Minute)}
	tenantID := uuid.New()
	result := &v1.QueueResults{TenantId: tenantID, Unassigned: []*sqlcv1.V1QueueItem{{TaskID: 42, ExternalID: uuid.New()}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.scheduleStepRuns(ctx, tenantID, result), context.Canceled)
	require.NoError(t, s.scheduleStepRuns(context.Background(), tenantID, result))
	filler, err := tasktypes.MonitoringEventMessageFromInternal(tenantID, tasktypes.CreateMonitoringEventPayload{TaskId: 1})
	require.NoError(t, err)
	require.NoError(t, buffer.Pub(context.Background(), msgqueue.OLAP_QUEUE, filler, true))
	require.EqualValues(t, 1, q.events.Load(), "failed enqueue must not suppress a later retry")
}

func TestQueueResultsApplyBackpressureAndDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan *v1.QueueResults, 4)
	started := make(chan struct{}, queueResultConcurrency+5)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var completed atomic.Int64
	wait := consumeQueueResults(ctx, results, func(*v1.QueueResults) {
		started <- struct{}{}
		<-release
		completed.Add(1)
	})
	for i := 0; i < queueResultConcurrency; i++ {
		results <- &v1.QueueResults{}
	}
	for i := 0; i < queueResultConcurrency; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("consumers did not start")
		}
	}
	for i := 0; i < cap(results); i++ {
		results <- &v1.QueueResults{}
	}
	sent := make(chan struct{})
	go func() { results <- &v1.QueueResults{}; close(results); close(sent) }()
	select {
	case <-sent:
		t.Error("result producer escaped the bounded channel")
	case <-started:
		t.Error("more handlers started than the concurrency limit")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("producer did not resume")
	}
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumers did not drain")
	}
	require.EqualValues(t, queueResultConcurrency+5, completed.Load())
}

func TestQueueResultsStopOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan *v1.QueueResults)
	wait := consumeQueueResults(ctx, results, func(*v1.QueueResults) { t.Error("unexpected result") })
	results <- nil
	cancel()
	done := make(chan struct{})
	go func() { wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumers did not stop")
	}
}

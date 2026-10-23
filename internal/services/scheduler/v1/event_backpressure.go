package scheduler

import (
	"context"
	"sync"

	"github.com/hashicorp/golang-lru/v2/expirable"
	v1 "github.com/hatchet-dev/hatchet/pkg/scheduling/v1"
)

const queueResultConcurrency = 32

// consumeQueueResults leaves excess results in the pool's bounded channel.
// Slow event publishing must apply backpressure instead of retaining a growing
// number of result batches in goroutine stacks.
func consumeQueueResults(ctx context.Context, results <-chan *v1.QueueResults, handle func(*v1.QueueResults)) func() {
	var wg sync.WaitGroup
	wg.Add(queueResultConcurrency)
	for i := 0; i < queueResultConcurrency; i++ {
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return
				case result, ok := <-results:
					if !ok {
						return
					}
					if result != nil {
						handle(result)
					}
				}
			}
		}()
	}
	return wg.Wait
}

type monitoringEventKey struct {
	cache *expirable.LRU[string, struct{}]
	task  string
}

// Reserve before publishing so concurrent retries cannot queue the same event
// behind a full buffer. Pending events stay reserved even if the TTL cache fills
// or expires while the publisher is blocked.
func (s *Scheduler) reserveMonitoringEvent(cache *expirable.LRU[string, struct{}], task string) bool {
	s.monitoringEventMu.Lock()
	defer s.monitoringEventMu.Unlock()
	key := monitoringEventKey{cache, task}
	if _, ok := s.pendingMonitoringEvents[key]; ok {
		return false
	}
	if _, ok := cache.Get(task); ok {
		return false
	}
	if s.pendingMonitoringEvents == nil {
		s.pendingMonitoringEvents = make(map[monitoringEventKey]struct{})
	}
	s.pendingMonitoringEvents[key] = struct{}{}
	return true
}

func (s *Scheduler) finishMonitoringEvent(cache *expirable.LRU[string, struct{}], task string, accepted bool) {
	s.monitoringEventMu.Lock()
	defer s.monitoringEventMu.Unlock()
	delete(s.pendingMonitoringEvents, monitoringEventKey{cache, task})
	if accepted {
		cache.Add(task, struct{}{})
	}
}

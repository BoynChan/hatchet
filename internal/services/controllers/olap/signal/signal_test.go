//go:build !e2e && !load && !rampup && !integration

package signal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	msgqueue "github.com/hatchet-dev/hatchet/internal/msgqueue"
	"github.com/hatchet-dev/hatchet/pkg/integrations/metrics/prometheus"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type recordingMQ struct {
	msgqueue.MessageQueue
	mu       sync.Mutex
	messages []*msgqueue.Message
}

func (r *recordingMQ) SendMessage(_ context.Context, _ msgqueue.Queue, msg *msgqueue.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, msg)
	return nil
}

type recordingPubSub struct {
	msgqueue.PubSub
	mu             sync.Mutex
	tenantMessages []*msgqueue.Message
}

func (r *recordingPubSub) Pub(_ context.Context, topic msgqueue.Topic, msg *msgqueue.Message) error {
	if topic.Kind() == msgqueue.TopicKindTenantStream {
		r.mu.Lock()
		r.tenantMessages = append(r.tenantMessages, msg)
		r.mu.Unlock()
	}
	return nil
}

type fakeTenantRepo struct {
	v1.TenantRepository
	tenant *sqlcv1.Tenant
}

func (f *fakeTenantRepo) GetTenantByID(_ context.Context, _ uuid.UUID) (*sqlcv1.Tenant, error) {
	return f.tenant, nil
}

type fakeRepo struct {
	v1.Repository
	tenantRepo v1.TenantRepository
}

func (f *fakeRepo) Tenant() v1.TenantRepository {
	return f.tenantRepo
}

func TestSignalTasksCreated_SkipsTenantStreamByDefault(t *testing.T) {
	tenantID := uuid.New()
	task := &v1.V1TaskWithPayload{
		V1Task: &sqlcv1.V1Task{
			ID:            101,
			TenantID:      tenantID,
			WorkflowRunID: uuid.New(),
			ExternalID:    uuid.New(),
			Queue:         "test-queue",
			InitialState:  sqlcv1.V1TaskInitialStateQUEUED,
			InsertedAt:    pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
		},
		Payload: []byte(`{"prompt":"hello"}`),
	}

	mq := &recordingMQ{}
	ps := &recordingPubSub{}
	repo := &fakeRepo{
		tenantRepo: &fakeTenantRepo{
			tenant: &sqlcv1.Tenant{ID: tenantID},
		},
	}
	l := zerolog.Nop()
	pubBuf := msgqueue.NewMQPubBuffer(mq)
	defer pubBuf.Stop()

	signaler := NewOLAPSignaler(mq, ps, repo, &l, pubBuf, prometheus.NewGate(nil, false, &l))

	// Default: SERVER_PUBLISH_CREATED_TASK_TENANT_STREAM unset -> must not publish created-task to tenant stream.
	t.Setenv("SERVER_PUBLISH_CREATED_TASK_TENANT_STREAM", "")
	err := signaler.SignalTasksCreated(context.Background(), tenantID, []*v1.V1TaskWithPayload{task})
	require.NoError(t, err)
	assert.Empty(t, ps.tenantMessages, "created-task tenant stream broadcast should be skipped by default")

	// Enabled via env: must publish created-task to tenant stream.
	t.Setenv("SERVER_PUBLISH_CREATED_TASK_TENANT_STREAM", "true")
	err = signaler.SignalTasksCreated(context.Background(), tenantID, []*v1.V1TaskWithPayload{task})
	require.NoError(t, err)
	require.Len(t, ps.tenantMessages, 1, "created-task tenant stream broadcast should be published when enabled")
	assert.Equal(t, msgqueue.MsgIDCreatedTask, ps.tenantMessages[0].ID)
}

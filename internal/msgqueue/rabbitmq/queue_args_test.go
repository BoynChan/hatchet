package rabbitmq

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

type stubQueue struct {
	name        string
	durable     bool
	autoDeleted bool
	exclusive   bool
	dlq         msgqueue.Queue
	isDLQ       bool
	isAutoDLQ   bool
	isExpirable bool
}

func (s stubQueue) Name() string        { return s.name }
func (s stubQueue) Durable() bool       { return s.durable }
func (s stubQueue) AutoDeleted() bool   { return s.autoDeleted }
func (s stubQueue) Exclusive() bool     { return s.exclusive }
func (s stubQueue) DLQ() msgqueue.Queue { return s.dlq }
func (s stubQueue) IsDLQ() bool         { return s.isDLQ }
func (s stubQueue) IsAutoDLQ() bool     { return s.isAutoDLQ }
func (s stubQueue) IsExpirable() bool   { return s.isExpirable }

func TestQueueDeclareArgs_ConsumerTimeoutOnlyWhenDurable(t *testing.T) {
	t.Run("durable static queue includes x-consumer-timeout and auto-DLX", func(t *testing.T) {
		args := queueDeclareArgs(msgqueue.TASK_PROCESSING_QUEUE)
		require.Equal(t, consumerTimeout.Milliseconds(), args["x-consumer-timeout"])
		require.Equal(t, "", args["x-dead-letter-exchange"])
		require.Equal(t, getTmpDLQName(msgqueue.TASK_PROCESSING_QUEUE.DLQ().Name()), args["x-dead-letter-routing-key"])
	})

	t.Run("durable exclusive dispatcher queue omits x-consumer-timeout and keeps TTLs", func(t *testing.T) {
		q := msgqueue.QueueTypeFromDispatcherID(uuid.New())
		args := queueDeclareArgs(q)
		_, hasTimeout := args["x-consumer-timeout"]
		require.False(t, hasTimeout)
		require.Equal(t, int32(20000), args["x-message-ttl"])
		require.Equal(t, int32(600000), args["x-expires"])
		require.Equal(t, msgqueue.DISPATCHER_DEAD_LETTER_QUEUE.Name(), args["x-dead-letter-routing-key"])
	})

	t.Run("non-durable expirable queue omits x-consumer-timeout", func(t *testing.T) {
		q := stubQueue{
			name:        "ephemeral-test",
			durable:     false,
			autoDeleted: true,
			isExpirable: true,
		}
		args := queueDeclareArgs(q)
		_, hasTimeout := args["x-consumer-timeout"]
		require.False(t, hasTimeout)
		require.Equal(t, int32(20000), args["x-message-ttl"])
		require.Equal(t, int32(600000), args["x-expires"])
	})

	t.Run("pubsub subscriber queue omits x-consumer-timeout", func(t *testing.T) {
		args := subQueueDeclareArgs()
		_, hasTimeout := args["x-consumer-timeout"]
		require.False(t, hasTimeout)
	})
}

func TestRabbitMQ_LiveQueueAndPubSubCompat(t *testing.T) {
	rawURL := os.Getenv("TEST_RABBITMQ_URL")
	if rawURL == "" {
		t.Skip("TEST_RABBITMQ_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cleanupMQ, mq, err := New(WithURL(rawURL))
	require.NoError(t, err)
	defer func() { _ = cleanupMQ() }()

	cleanupPS, ps, err := NewPubSub(WithPubSubURL(rawURL))
	require.NoError(t, err)
	defer func() { _ = cleanupPS() }()

	staticQ := msgqueue.NewRandomStaticQueue()
	dispQ := msgqueue.QueueTypeFromDispatcherID(uuid.New())
	tenantID := uuid.New()
	tenantTopic := msgqueue.TenantTopic(tenantID)
	schedTopic := msgqueue.SchedulerPartitionTopic("compat-test-" + uuid.NewString())

	defer func() {
		conn, err := amqp.Dial(rawURL)
		if err != nil {
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer ch.Close()
		for _, qn := range []string{
			staticQ.Name(),
			getTmpDLQName(staticQ.DLQ().Name()),
			getProcDLQName(staticQ.DLQ().Name()),
		} {
			_, _ = ch.QueueDelete(qn, false, false, false)
		}
		_ = ch.ExchangeDelete(tenantTopic.Name(), false, false)
	}()

	// 1. Durable static queue (with auto-DLX + x-consumer-timeout)
	gotStatic := make(chan string, 1)
	unsubStatic, err := mq.Subscribe(staticQ, func(msg *msgqueue.Message) error {
		gotStatic <- msg.ID
		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	defer func() { _ = unsubStatic() }()

	require.NoError(t, mq.SendMessage(ctx, staticQ, &msgqueue.Message{
		ID:         "static-ok",
		Payloads:   [][]byte{[]byte(`{"k":"v"}`)},
		Persistent: true,
	}))
	select {
	case id := <-gotStatic:
		require.Equal(t, "static-ok", id)
	case <-ctx.Done():
		t.Fatal("timed out waiting for durable static queue message")
	}

	// 2. Durable exclusive expirable dispatcher queue (with TTL, no x-consumer-timeout)
	gotDisp := make(chan string, 1)
	unsubDisp, err := mq.Subscribe(dispQ, func(msg *msgqueue.Message) error {
		gotDisp <- msg.ID
		return nil
	}, msgqueue.NoOpHook)
	require.NoError(t, err)
	defer func() { _ = unsubDisp() }()
	time.Sleep(1 * time.Second)

	require.NoError(t, mq.SendMessage(ctx, dispQ, &msgqueue.Message{
		ID:       "disp-ok",
		Payloads: [][]byte{[]byte(`{"k":"v"}`)},
	}))
	select {
	case id := <-gotDisp:
		require.Equal(t, "disp-ok", id)
	case <-ctx.Done():
		t.Fatal("timed out waiting for dispatcher queue message")
	}

	// 3. PubSub Tenant fanout topic (non-durable exclusive auto-delete)
	gotTenant := make(chan string, 1)
	unsubTenant, err := ps.Sub(tenantTopic, func(msg *msgqueue.Message) error {
		gotTenant <- msg.ID
		return nil
	})
	require.NoError(t, err)
	defer func() { _ = unsubTenant() }()
	time.Sleep(1 * time.Second)

	require.NoError(t, ps.Pub(ctx, tenantTopic, &msgqueue.Message{
		ID:       "tenant-ok",
		Payloads: [][]byte{[]byte(`{"k":"v"}`)},
	}))
	select {
	case id := <-gotTenant:
		require.Equal(t, "tenant-ok", id)
	case <-ctx.Done():
		t.Fatal("timed out waiting for tenant pubsub message")
	}

	// 4. PubSub SchedulerPartition topic (non-durable exclusive auto-delete)
	gotSched := make(chan string, 1)
	unsubSched, err := ps.Sub(schedTopic, func(msg *msgqueue.Message) error {
		gotSched <- msg.ID
		return nil
	})
	require.NoError(t, err)
	defer func() { _ = unsubSched() }()
	time.Sleep(1 * time.Second)

	require.NoError(t, ps.Pub(ctx, schedTopic, &msgqueue.Message{
		ID:       "sched-ok",
		Payloads: [][]byte{[]byte(`{"k":"v"}`)},
	}))
	select {
	case id := <-gotSched:
		require.Equal(t, "sched-ok", id)
	case <-ctx.Done():
		t.Fatal("timed out waiting for scheduler pubsub message")
	}
}

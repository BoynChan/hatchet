package msgqueue

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicNames(t *testing.T) {
	tenantId := uuid.MustParse("707d0855-80ab-4e1f-a156-f1c4546cbf52")

	tenantTopic := TenantTopic(tenantId)
	assert.Equal(t, "707d0855-80ab-4e1f-a156-f1c4546cbf52_v1", tenantTopic.Name())
	assert.Equal(t, TopicKindTenantStream, tenantTopic.Kind())

	schedulerTopic := SchedulerPartitionTopic("mypartition")
	assert.Equal(t, "mypartition_scheduler_v1", schedulerTopic.Name())
	assert.Equal(t, TopicKindSchedulerPartition, schedulerTopic.Kind())
}

type recordingPubSub struct {
	pubs     []string
	messages []*Message
}

func (r *recordingPubSub) Pub(ctx context.Context, topic Topic, msg *Message) error {
	r.pubs = append(r.pubs, topic.Name()+"/"+msg.ID)
	r.messages = append(r.messages, msg)
	return nil
}

func TestPubTenantMessageCompactsCreatedTask(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "compressed"}[compressed], func(t *testing.T) {
			fields := map[string]any{
				"workflow_run_id": uuid.NewString(), "external_id": uuid.NewString(),
				"inserted_at": "2026-01-01T00:00:00.123456Z", "retry_count": 3,
				"payload": strings.Repeat("private-input", 2000), "additional_metadata": "private-metadata",
			}
			msg, err := NewTenantMessage(uuid.New(), MsgIDCreatedTask, false, true, fields, fields)
			require.NoError(t, err)
			if compressed {
				result, err := (Compressor{Enabled: true, Threshold: 1}).CompressPayloads(msg.Payloads)
				require.NoError(t, err)
				msg.Payloads, msg.Compressed = result.Payloads, result.WasCompressed
			}
			original, err := json.Marshal(msg)
			require.NoError(t, err)
			var durableBytes []byte
			mq := &mockMessageQueue{sendMessageFn: func(_ context.Context, _ Queue, sent *Message) error {
				var err error
				durableBytes, err = json.Marshal(sent)
				return err
			}}
			ps := &recordingPubSub{}
			l := zerolog.Nop()
			require.NoError(t, PubTenantMessage(context.Background(), &l, mq, ps, OLAP_QUEUE, msg))
			require.Equal(t, original, durableBytes)
			after, err := json.Marshal(msg)
			require.NoError(t, err)
			require.Equal(t, original, after, "stream projection must not mutate the durable message")
			require.Len(t, ps.messages, 1)
			stream := ps.messages[0]
			require.False(t, stream.Compressed)
			require.Equal(t, msg.TenantID, stream.TenantID)
			require.Len(t, stream.Payloads, 2)
			delete(fields, "payload")
			delete(fields, "additional_metadata")
			expected, err := json.Marshal(fields)
			require.NoError(t, err)
			for _, payload := range stream.Payloads {
				require.JSONEq(t, string(expected), string(payload))
			}
		})
	}
}

func TestPubTenantMessageProjectionErrorsPreserveDurableDelivery(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		msg := &Message{ID: MsgIDCreatedTask, TenantID: uuid.New(), Payloads: [][]byte{[]byte("invalid")}, Compressed: compressed}
		ps := &recordingPubSub{}
		l := zerolog.Nop()
		var durableCalls int
		mq := &mockMessageQueue{sendMessageFn: func(context.Context, Queue, *Message) error { durableCalls++; return nil }}
		require.NoError(t, PubTenantMessage(context.Background(), &l, mq, ps, OLAP_QUEUE, msg))
		require.Equal(t, 1, durableCalls)
		require.Empty(t, ps.messages)
		require.Error(t, PubTenantMessage(context.Background(), &l, nil, ps, nil, msg))
		require.Empty(t, ps.messages)
	}
}

func (r *recordingPubSub) Sub(topic Topic, handler MsgHandler) (func() error, error) {
	return func() error { return nil }, nil
}

func (r *recordingPubSub) IsReady() bool {
	return true
}

func TestPubTenantMessage(t *testing.T) {
	tenantId := uuid.New()
	l := zerolog.Nop()

	t.Run("stream-consumed id is published", func(t *testing.T) {
		inner := &recordingPubSub{}

		err := PubTenantMessage(context.Background(), &l, nil, inner, nil, &Message{ID: MsgIDTaskCompleted, TenantID: tenantId})
		assert.NoError(t, err)
		assert.Len(t, inner.pubs, 1)
	})

	t.Run("non-stream id is not published", func(t *testing.T) {
		inner := &recordingPubSub{}

		err := PubTenantMessage(context.Background(), &l, nil, inner, nil, &Message{ID: MsgIDTaskTrigger, TenantID: tenantId})
		assert.NoError(t, err)
		assert.Empty(t, inner.pubs)
	})

	t.Run("nil tenant id is not published", func(t *testing.T) {
		inner := &recordingPubSub{}

		err := PubTenantMessage(context.Background(), &l, nil, inner, nil, &Message{ID: MsgIDTaskCompleted, TenantID: uuid.Nil})
		assert.NoError(t, err)
		assert.Empty(t, inner.pubs)
	})

	t.Run("durable send happens before stream publish", func(t *testing.T) {
		inner := &recordingPubSub{}
		var sent []string
		mq := &mockMessageQueue{sendMessageFn: func(ctx context.Context, q Queue, msg *Message) error {
			sent = append(sent, q.Name()+"/"+msg.ID)
			return nil
		}}

		err := PubTenantMessage(context.Background(), &l, mq, inner, TASK_PROCESSING_QUEUE, &Message{ID: MsgIDTaskFailed, TenantID: tenantId})
		assert.NoError(t, err)
		assert.Len(t, sent, 1)
		assert.Len(t, inner.pubs, 1)
	})

	t.Run("durable error is returned and skips the stream publish", func(t *testing.T) {
		inner := &recordingPubSub{}
		mq := &mockMessageQueue{sendMessageFn: func(ctx context.Context, q Queue, msg *Message) error {
			return assert.AnError
		}}

		err := PubTenantMessage(context.Background(), &l, mq, inner, TASK_PROCESSING_QUEUE, &Message{ID: MsgIDTaskFailed, TenantID: tenantId})
		assert.Error(t, err)
		assert.Empty(t, inner.pubs)
	})

	t.Run("stream publish error is swallowed when durably sent", func(t *testing.T) {
		inner := &erroringPubSub{}
		mq := &mockMessageQueue{}

		err := PubTenantMessage(context.Background(), &l, mq, inner, TASK_PROCESSING_QUEUE, &Message{ID: MsgIDTaskFailed, TenantID: tenantId})
		assert.NoError(t, err)
	})

	t.Run("stream publish error is returned when it is the only delivery path", func(t *testing.T) {
		inner := &erroringPubSub{}

		err := PubTenantMessage(context.Background(), &l, nil, inner, nil, &Message{ID: MsgIDTaskStreamEvent, TenantID: tenantId})
		assert.Error(t, err)
	})
}

type erroringPubSub struct{}

func (e *erroringPubSub) Pub(ctx context.Context, topic Topic, msg *Message) error {
	return assert.AnError
}

func (e *erroringPubSub) Sub(topic Topic, handler MsgHandler) (func() error, error) {
	return func() error { return nil }, nil
}

func (e *erroringPubSub) IsReady() bool {
	return true
}

func TestGatedPubSub(t *testing.T) {
	tenantId := uuid.New()

	cases := []struct {
		name      string
		disabled  bool
		topic     Topic
		msgId     string
		delivered bool
	}{
		{"enabled tenant other", false, TenantTopic(tenantId), MsgIDTaskCompleted, true},
		{"enabled tenant stream", false, TenantTopic(tenantId), MsgIDTaskStreamEvent, true},
		{"enabled scheduler", false, SchedulerPartitionTopic("p"), MsgIDCheckTenantQueue, true},
		{"disabled tenant other", true, TenantTopic(tenantId), MsgIDTaskCompleted, false},
		{"disabled tenant stream", true, TenantTopic(tenantId), MsgIDTaskStreamEvent, true},
		{"disabled scheduler", true, SchedulerPartitionTopic("p"), MsgIDCheckTenantQueue, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &recordingPubSub{}
			ps := NewGatedPubSub(inner, tc.disabled)

			err := ps.Pub(context.Background(), tc.topic, &Message{ID: tc.msgId, TenantID: tenantId})
			assert.NoError(t, err)

			if tc.delivered {
				assert.Len(t, inner.pubs, 1)
			} else {
				assert.Empty(t, inner.pubs)
			}
		})
	}
}

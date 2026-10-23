//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	"github.com/hatchet-dev/hatchet/pkg/repository"
)

func TestPostgresNotifyByteBoundary(t *testing.T) {
	ps, pool := newTestPubSub(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	listener, err := pgx.Connect(ctx, testDatabaseURL())
	require.NoError(t, err)
	defer listener.Close(context.Background())
	_, err = listener.Exec(ctx, "LISTEN hatchet_listener")
	require.NoError(t, err)

	for _, utf8 := range []bool{false, true} {
		for _, size := range []int{7999, 8000, 8001} {
			t.Run(fmt.Sprintf("bytes_%d_utf8_%t", size, utf8), func(t *testing.T) {
				queue := uuid.NewString()
				require.NoError(t, ps.repo.BindQueue(ctx, queue, false, true, false, nil))
				t.Cleanup(func() {
					_, err := pool.Exec(context.Background(), `DELETE FROM "MessageQueueItem" WHERE "queueId" = $1`, queue)
					require.NoError(t, err)
					_, err = pool.Exec(context.Background(), `DELETE FROM "MessageQueue" WHERE name = $1`, queue)
					require.NoError(t, err)
				})
				empty, err := json.Marshal(repository.PubSubMessage{QueueName: queue, Payload: json.RawMessage(`{"data":""}`)})
				require.NoError(t, err)
				padding := size - len(empty)
				data := strings.Repeat("a", padding)
				if utf8 {
					data = strings.Repeat("界", padding/3) + strings.Repeat("a", padding%3)
				}
				payload := `{"data":"` + data + `"}`
				wrapped, err := json.Marshal(repository.PubSubMessage{QueueName: queue, Payload: json.RawMessage(payload)})
				require.NoError(t, err)
				require.Len(t, wrapped, size)
				require.NoError(t, ps.repo.Notify(ctx, queue, payload))
				rows, err := ps.repo.ReadMessages(ctx, queue, 10)
				require.NoError(t, err)
				if size < 8000 {
					require.Empty(t, rows)
					notification, err := listener.WaitForNotification(ctx)
					require.NoError(t, err)
					require.JSONEq(t, string(wrapped), notification.Payload)
				} else {
					require.Len(t, rows, 1)
					require.JSONEq(t, payload, string(rows[0].Payload))
					require.NoError(t, ps.repo.AckMessage(ctx, rows[0].ID))
				}
			})
		}
	}
}

type deletingQueueRepository struct {
	repository.MessageQueueRepository
	pool  *pgxpool.Pool
	calls int
}

func (r *deletingQueueRepository) Notify(ctx context.Context, queue, payload string) error {
	r.calls++
	if r.calls == 1 {
		if _, err := r.pool.Exec(ctx, `DELETE FROM "MessageQueue" WHERE name = $1`, queue); err != nil {
			return err
		}
	}
	return r.MessageQueueRepository.Notify(ctx, queue, payload)
}

func TestPostgresPubSubQueueLifecycle(t *testing.T) {
	for _, deleteDuringPublish := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_during_publish_%t", deleteDuringPublish), func(t *testing.T) {
			ps, pool := newTestPubSub(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			topic := msgqueue.TenantTopic(uuid.New())
			require.NoError(t, ps.ensureQueue(ctx, topic))
			if deleteDuringPublish {
				ps.repo = &deletingQueueRepository{MessageQueueRepository: ps.repo, pool: pool}
			} else {
				_, err := pool.Exec(ctx, `DELETE FROM "MessageQueue" WHERE name = $1`, topic.Name())
				require.NoError(t, err)
			}
			msg, err := msgqueue.NewTenantMessage(uuid.New(), msgqueue.MsgIDTaskStreamEvent, false, false, strings.Repeat("x", 10000))
			require.NoError(t, err)
			require.NoError(t, ps.Pub(ctx, topic, msg))
			rows, err := ps.repo.ReadMessages(ctx, topic.Name(), 10)
			require.NoError(t, err)
			require.Len(t, rows, 1, "retry must enqueue exactly one message")
			var received msgqueue.Message
			require.NoError(t, json.Unmarshal(rows[0].Payload, &received))
			require.Equal(t, msg.Payloads, received.Payloads)
			require.NoError(t, ps.repo.AckMessage(ctx, rows[0].ID))
			if deleting, ok := ps.repo.(*deletingQueueRepository); ok {
				require.Equal(t, 2, deleting.calls)
			}

			// A publishing-only topic must refresh its activity when its binding expires.
			_, err = pool.Exec(ctx, `UPDATE "MessageQueue" SET "lastActive" = NOW() - INTERVAL '2 hours' WHERE name = $1`, topic.Name())
			require.NoError(t, err)
			ps.ttlCache.Remove(topic.Name())
			require.NoError(t, ps.Pub(ctx, topic, &msgqueue.Message{Payloads: [][]byte{[]byte(`{}`)}}))
			require.NoError(t, ps.repo.CleanupQueues(ctx))
			var exists bool
			require.NoError(t, pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM "MessageQueue" WHERE name = $1 AND "lastActive" > NOW() - INTERVAL '1 minute')`, topic.Name()).Scan(&exists))
			require.True(t, exists)
			_, err = pool.Exec(ctx, `DELETE FROM "MessageQueue" WHERE name = $1`, topic.Name())
			require.NoError(t, err)
		})
	}
}

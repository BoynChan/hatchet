package sqlcv1

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestReleaseTasksRuntimePresence(t *testing.T) {
	dsn := os.Getenv("HATCHET_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("HATCHET_TEST_DATABASE_URL is required for PostgreSQL regression test")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
CREATE TEMP TABLE v1_task (
 id bigint, inserted_at timestamptz, queue text, external_id uuid, step_readable_id text,
 workflow_run_id uuid, retry_count int, concurrency_strategy_ids bigint[], idempotency_key text,
 is_dag_orchestrator boolean
);
CREATE TEMP TABLE v1_task_runtime (
 task_id bigint, task_inserted_at timestamptz, retry_count int, worker_id uuid, batch_id uuid, batch_key text
);
CREATE TEMP TABLE v1_task_runtime_slot (task_id bigint, task_inserted_at timestamptz, retry_count int);
CREATE TEMP TABLE v1_queue_item (LIKE v1_task_runtime_slot);
CREATE TEMP TABLE v1_batched_queue_item (LIKE v1_task_runtime_slot);
CREATE TEMP TABLE v1_rate_limited_queue_items (LIKE v1_task_runtime_slot);
CREATE TEMP TABLE v1_paused_workflow_queue_item (LIKE v1_task_runtime_slot);
CREATE TEMP TABLE v1_retry_queue_item (task_id bigint, task_inserted_at timestamptz, task_retry_count int);
CREATE TEMP TABLE v1_concurrency_slot (
 task_id bigint, task_inserted_at timestamptz, task_retry_count int,
 parent_strategy_id bigint, workflow_version_id uuid, workflow_run_id uuid
);
CREATE TEMP TABLE v1_workflow_concurrency_slot (
 sort_id bigint, tenant_id uuid, workflow_id uuid, workflow_version_id uuid, workflow_run_id uuid,
 strategy_id bigint, completed_child_strategy_ids bigint[], child_strategy_ids bigint[], priority int,
 key text, is_filled boolean
);
`)
	require.NoError(t, err)
	inserted := pgtype.Timestamptz{Time: time.Now().UTC().Truncate(time.Microsecond), Valid: true}
	worker := uuid.New()
	for id := int64(1); id <= 3; id++ {
		retry := 0
		if id == 3 {
			retry = 1
		}
		_, err = tx.Exec(ctx, `INSERT INTO v1_task VALUES ($1,$2,'queue',$3,'step',$4,$5,'{}',NULL,false)`, id, inserted, uuid.New(), uuid.New(), retry)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `INSERT INTO v1_task_runtime VALUES ($1,$2,0,$3,NULL,NULL);`, id, inserted, worker)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, `INSERT INTO v1_task_runtime_slot VALUES ($1,$2,0);`, id, inserted)
		require.NoError(t, err)
	}
	// Model manual slot release, which keeps the runtime and clears its worker.
	_, err = tx.Exec(ctx, `DELETE FROM v1_task_runtime_slot WHERE task_id=2; UPDATE v1_task_runtime SET worker_id=NULL WHERE task_id=2;`)
	require.NoError(t, err)
	q := New()
	args := ReleaseTasksParams{Taskids: []int64{1, 2, 3}, Taskinsertedats: []pgtype.Timestamptz{inserted, inserted, inserted}, Retrycounts: []int32{0, 0, 0}}
	rows, err := q.ReleaseTasks(ctx, tx, args)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	byID := make(map[int64]*ReleaseTasksRow)
	for _, row := range rows {
		byID[row.ID] = row
		require.True(t, row.HadRuntime)
	}
	require.Equal(t, worker, byID[1].WorkerID)
	require.Equal(t, uuid.Nil, byID[2].WorkerID)
	require.True(t, byID[2].IsCurrentRetry)
	require.False(t, byID[3].IsCurrentRetry)
	rows, err = q.ReleaseTasks(ctx, tx, args)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	for _, row := range rows {
		require.False(t, row.HadRuntime)
	}
}

//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestManualSlotRelease(t *testing.T) {
	pool, cleanup := setupPostgresImageWithMigration(t, "postgres:17.7")
	t.Cleanup(cleanup)
	repo := createTaskRepository(pool)
	ctx := context.Background()

	t.Run("releases_all_slot_types_while_task_remains_running", func(t *testing.T) {
		f := seedSlotReleaseTask(t, pool)
		capacity := func() int32 {
			rows, err := sqlcv1.New().ListAvailableSlotsForWorkers(ctx, pool, sqlcv1.ListAvailableSlotsForWorkersParams{
				Tenantid: f.tenantID, Workerids: []uuid.UUID{f.workerID}, Slottype: "default",
			})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			return rows[0].AvailableSlots
		}
		require.Equal(t, int32(1), capacity())
		released, err := repo.ReleaseSlot(ctx, f.tenantID, f.externalID)
		require.NoError(t, err)
		require.NotNil(t, released.WorkerID)
		require.Equal(t, f.workerID, *released.WorkerID, "release event must identify the worker whose capacity was freed")
		assertReleasedRuntime(t, pool, f)
		require.Equal(t, int32(3), capacity(), "scheduler must see reusable capacity before completion")
		_, err = pool.Exec(ctx, `DELETE FROM v1_task_runtime WHERE task_id=$1`, f.taskID)
		require.NoError(t, err)
		require.Equal(t, int32(3), capacity(), "completion must not release capacity twice")
	})

	t.Run("repeated_and_concurrent_calls_are_idempotent", func(t *testing.T) {
		f := seedSlotReleaseTask(t, pool)
		errs := make(chan error, 8)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				_, err := repo.ReleaseSlot(ctx, f.tenantID, f.externalID)
				errs <- err
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		assertReleasedRuntime(t, pool, f)
	})

	t.Run("cannot_release_another_tenant", func(t *testing.T) {
		f := seedSlotReleaseTask(t, pool)
		_, err := repo.ReleaseSlot(ctx, uuid.New(), f.externalID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		var slots int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM v1_task_runtime_slot WHERE task_id=$1`, f.taskID).Scan(&slots))
		require.Equal(t, 2, slots)
	})

	t.Run("does_not_release_other_attempts_or_tasks", func(t *testing.T) {
		f := seedSlotReleaseTask(t, pool)
		other := seedSlotReleaseTask(t, pool)
		_, err := pool.Exec(ctx, `INSERT INTO v1_task_runtime_slot
			(tenant_id, task_id, task_inserted_at, retry_count, worker_id, slot_type, units)
			VALUES ($1,$2,$3,1,$4,'default',1)`, f.tenantID, f.taskID, f.insertedAt, f.workerID)
		require.NoError(t, err)
		_, err = repo.ReleaseSlot(ctx, f.tenantID, f.externalID)
		require.NoError(t, err)
		assertReleasedRuntime(t, pool, f)
		var slots int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM v1_task_runtime_slot WHERE (task_id=$1 AND retry_count=1) OR task_id=$2`, f.taskID, other.taskID).Scan(&slots))
		require.Equal(t, 3, slots)
	})

	t.Run("missing_runtime_is_not_found", func(t *testing.T) {
		f := seedSlotReleaseTask(t, pool)
		_, err := pool.Exec(ctx, `DELETE FROM v1_task_runtime WHERE task_id=$1`, f.taskID)
		require.NoError(t, err)
		_, err = repo.ReleaseSlot(ctx, f.tenantID, f.externalID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
	})

	t.Run("update_failure_rolls_back_slot_deletion", func(t *testing.T) {
		f := seedSlotReleaseTask(t, pool)
		_, err := pool.Exec(ctx, `CREATE FUNCTION reject_manual_slot_release_test() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'injected runtime update failure'; END; $$;
			CREATE TRIGGER reject_manual_slot_release_test BEFORE UPDATE OF worker_id ON v1_task_runtime
			FOR EACH ROW EXECUTE FUNCTION reject_manual_slot_release_test()`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := pool.Exec(ctx, `DROP TRIGGER reject_manual_slot_release_test ON v1_task_runtime; DROP FUNCTION reject_manual_slot_release_test()`)
			require.NoError(t, err)
		})
		_, err = repo.ReleaseSlot(ctx, f.tenantID, f.externalID)
		require.ErrorContains(t, err, "injected runtime update failure")
		var slots int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM v1_task_runtime_slot WHERE task_id=$1`, f.taskID).Scan(&slots))
		require.Equal(t, 2, slots)
		var workerID uuid.UUID
		require.NoError(t, pool.QueryRow(ctx, `SELECT worker_id FROM v1_task_runtime WHERE task_id=$1`, f.taskID).Scan(&workerID))
		require.Equal(t, f.workerID, workerID)
	})
}

type slotReleaseFixture struct {
	tenantID, workerID, externalID uuid.UUID
	taskID                         int64
	insertedAt                     time.Time
}

func seedSlotReleaseTask(t *testing.T, pool *pgxpool.Pool) slotReleaseFixture {
	t.Helper()
	ctx := context.Background()
	f := slotReleaseFixture{tenantID: uuid.New(), workerID: uuid.New(), externalID: uuid.New()}
	_, err := pool.Exec(ctx, `INSERT INTO "Tenant" (id,name,slug) VALUES ($1,'slot-release-test',$2)`, f.tenantID, f.tenantID.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO "Worker" (id,"tenantId",name,"maxRuns") VALUES ($1,$2,'slot-release-worker',3)`, f.workerID, f.tenantID)
	require.NoError(t, err)
	err = pool.QueryRow(ctx, `INSERT INTO v1_task
		(tenant_id,queue,action_id,step_id,step_readable_id,workflow_id,workflow_version_id,workflow_run_id,schedule_timeout,step_timeout,sticky,external_id,display_name,input,step_index)
		VALUES ($1,'release-test','release:run',$2,'run',$3,$4,$5,'5m','1m','NONE',$6,'release-test','{}',0)
		RETURNING id,inserted_at`, f.tenantID, uuid.New(), uuid.New(), uuid.New(), uuid.New(), f.externalID).Scan(&f.taskID, &f.insertedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO v1_lookup_table (tenant_id,external_id,task_id,inserted_at) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, f.tenantID, f.externalID, f.taskID, f.insertedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO v1_task_runtime (tenant_id,task_id,task_inserted_at,retry_count,worker_id,timeout_at) VALUES ($1,$2,$3,0,$4,NOW()+INTERVAL '1 minute')`, f.tenantID, f.taskID, f.insertedAt, f.workerID)
	require.NoError(t, err)
	// Explicit reservations exercise multiple slot types and multi-unit costs.
	_, err = pool.Exec(ctx, `INSERT INTO v1_task_runtime_slot (tenant_id,task_id,task_inserted_at,retry_count,worker_id,slot_type,units)
		VALUES ($1,$2,$3,0,$4,'default',2),($1,$2,$3,0,$4,'custom',1)
		ON CONFLICT (task_id,task_inserted_at,retry_count,slot_type) DO UPDATE SET units=EXCLUDED.units`, f.tenantID, f.taskID, f.insertedAt, f.workerID)
	require.NoError(t, err)
	return f
}

func assertReleasedRuntime(t *testing.T, pool *pgxpool.Pool, f slotReleaseFixture) {
	t.Helper()
	ctx := context.Background()
	var slots int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM v1_task_runtime_slot WHERE task_id=$1 AND task_inserted_at=$2 AND retry_count=0`, f.taskID, f.insertedAt).Scan(&slots))
	require.Zero(t, slots, "successful release must remove physical reservations before task completion")
	var unassigned, timeoutPreserved bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT worker_id IS NULL, timeout_at > NOW() FROM v1_task_runtime WHERE task_id=$1 AND task_inserted_at=$2 AND retry_count=0`, f.taskID, f.insertedAt).Scan(&unassigned, &timeoutPreserved))
	require.True(t, unassigned)
	require.True(t, timeoutPreserved, "released tasks still need execution timeout recovery")
}

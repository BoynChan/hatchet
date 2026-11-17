package dispatcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts/contractsconnect"
	"github.com/hatchet-dev/hatchet/pkg/analytics"
	"github.com/hatchet-dev/hatchet/pkg/config/limits"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// Run against a migrated disposable database and a Python interpreter with the SDK
// under test installed. Both faults are injected on real gRPC messages.
func TestRecoverWorkerStart(t *testing.T) {
	dsn, python := os.Getenv("HATCHET_TEST_DATABASE_URL"), os.Getenv("HATCHET_TEST_PYTHON")
	if dsn == "" || python == "" {
		t.Skip("set HATCHET_TEST_DATABASE_URL and HATCHET_TEST_PYTHON")
	}
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	log := zerolog.Nop()
	repo, cleanup := repository.NewRepository(pool, pool, &log, time.Minute, time.Hour, time.Hour, 3,
		repository.TaskOperationLimits{}, repository.PayloadStoreRepositoryOpts{}, repository.StatusUpdateBatchSizeLimits{}, limits.LimitConfigFile{}, false, repository.DurableEventBufferOpts{})
	defer cleanup()
	tenant, dispatcher := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO "Tenant"(id,name,slug) VALUES($1,'delivery-test',$2)`, tenant, tenant.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO "Dispatcher"(id,"lastHeartbeatAt") VALUES($1,now())`, dispatcher)
	require.NoError(t, err)
	engine := &DispatcherImpl{repov1: repo, workers: &workers{}, dispatcherId: dispatcher, l: &log, analytics: analytics.NoOpAnalytics{}, defaultMaxWorkerLockAcquisitionTime: time.Second}
	_, handler := contractsconnect.NewDispatcherHandler(engine)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), "tenant", &sqlcv1.Tenant{ID: tenant})))
	}))
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()
	defer server.Close()

	for _, scenario := range []string{"lost_start", "lost_reply", "lost_receipt", "heartbeat_replay"} {
		t.Run(scenario, func(t *testing.T) {
			worker := uuid.New()
			_, err := pool.Exec(ctx, `INSERT INTO "Worker"(id,"tenantId","dispatcherId",name,"lastHeartbeatAt") VALUES($1,$2,$3,'delivery-test',now())`, worker, tenant, dispatcher)
			require.NoError(t, err)
			target, barrier := uuid.New(), uuid.New()
			child := exec.CommandContext(t.Context(), python, "testdata/delivery_worker.py", strings.TrimPrefix(server.URL, "http://"), worker.String(), scenario, target.String(), barrier.String())
			var output strings.Builder
			child.Stdout = &output
			child.Stderr = &output
			require.NoError(t, child.Start())
			defer func() {
				if child.ProcessState == nil {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
				t.Log(output.String())
			}()
			require.Eventually(t, func() bool { _, err := engine.workers.Get(worker); return err == nil }, 10*time.Second, 10*time.Millisecond)
			release := sqlcv1.ReleaseTasksParams{}
			var delivery, receipt, session uuid.UUID
			for _, external := range []uuid.UUID{target, barrier} {
				task := &sqlcv1.V1Task{ExternalID: external, TenantID: tenant, WorkflowRunID: uuid.New(), StepID: uuid.New(), ActionID: "test:execute", StepReadableID: "execute"}
				// The scheduler has already committed the assignment when dispatch begins.
				err = pool.QueryRow(ctx, `INSERT INTO v1_task_runtime(task_id,task_inserted_at,retry_count,tenant_id,worker_id,timeout_at)
					VALUES(nextval('v1_task_id_seq'),now(),0,$1,$2,now()+interval '30 minutes') RETURNING task_id,task_inserted_at`, tenant, worker).Scan(&task.ID, &task.InsertedAt)
				require.NoError(t, err)
				_, err = pool.Exec(ctx, `INSERT INTO v1_task_runtime_slot(tenant_id,task_id,task_inserted_at,retry_count,worker_id,slot_type,units)
                    VALUES($1,$2,$3,0,$4,'default',1)`, tenant, task.ID, task.InsertedAt, worker)
				require.NoError(t, err)
				release.Taskids = append(release.Taskids, task.ID)
				release.Taskinsertedats = append(release.Taskinsertedats, task.InsertedAt)
				release.Retrycounts = append(release.Retrycounts, task.RetryCount)
				active, err := engine.workers.Get(worker)
				require.NoError(t, err)
				require.NoError(t, active[0].StartTaskFromBulk(ctx, tenant, &repository.V1TaskWithPayload{V1Task: task, Payload: []byte("{}")}, nil))
				require.Eventually(t, func() bool {
					var claimed bool
					err := pool.QueryRow(ctx, `SELECT receipt_id IS NOT NULL FROM v1_worker_delivery WHERE task_id=$1 AND task_inserted_at=$2 AND retry_count=0`, task.ID, task.InsertedAt).Scan(&claimed)
					return err == nil && claimed
				}, 12*time.Second, 20*time.Millisecond)
				if external == target {
					// Send a duplicate over the current listener after the claim committed.
					active, err = engine.workers.Get(worker)
					require.NoError(t, err)
					session = active[0].sessionID
					require.NoError(t, active[0].StartTaskFromBulk(ctx, tenant, &repository.V1TaskWithPayload{V1Task: task, Payload: []byte("{}")}, nil))
					err = pool.QueryRow(ctx, `SELECT delivery_id, receipt_id FROM v1_worker_delivery WHERE task_id=$1`, task.ID).Scan(&delivery, &receipt)
					require.NoError(t, err)
				}
			}
			require.NoError(t, child.Wait())
			_, err = sqlcv1.New().ReleaseTasks(ctx, pool, release)
			require.NoError(t, err)
			var remaining int
			err = pool.QueryRow(ctx, `SELECT
                (SELECT count(*) FROM v1_task_runtime_slot WHERE worker_id=$1) +
                (SELECT count(*) FROM v1_worker_delivery WHERE worker_id=$1)`, worker).Scan(&remaining)
			require.NoError(t, err)
			require.Zero(t, remaining)
			accepted, err := repo.Workers().ClaimWorkerDelivery(ctx, tenant, worker, session, delivery, receipt)
			require.NoError(t, err)
			require.False(t, accepted, "a retired assignment cannot authorize execution")
		})
	}
}

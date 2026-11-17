package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// WorkerDeliveries is the delivery state of individual, non-durable task attempts.
type WorkerDeliveries interface {
	SaveWorkerDelivery(context.Context, uuid.UUID, uuid.UUID, *sqlcv1.V1Task, []byte) (uuid.UUID, error)
	PendingWorkerDeliveries(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) ([]WorkerDelivery, error)
	ClaimWorkerDelivery(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID) (bool, error)
}

type WorkerDelivery struct {
	ID     uuid.UUID
	Action []byte
}

func (w *workerRepository) SaveWorkerDelivery(ctx context.Context, tenant, worker uuid.UUID, task *sqlcv1.V1Task, action []byte) (uuid.UUID, error) {
	var id uuid.UUID
	err := w.pool.QueryRow(ctx, `
		INSERT INTO v1_worker_delivery AS d
			(task_id, task_inserted_at, retry_count, tenant_id, worker_id, action)
		SELECT task_id, task_inserted_at, retry_count, tenant_id, worker_id, $6
		FROM v1_task_runtime
		WHERE tenant_id = $1 AND worker_id = $2 AND task_id = $3
			AND task_inserted_at = $4 AND retry_count = $5 AND timeout_at > now()
		ON CONFLICT (task_id, task_inserted_at, retry_count) DO UPDATE
			SET delivery_id = d.delivery_id WHERE d.worker_id = excluded.worker_id
		RETURNING delivery_id`, tenant, worker, task.ID, task.InsertedAt, task.RetryCount, action).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil
	}
	return id, err
}

func (w *workerRepository) PendingWorkerDeliveries(ctx context.Context, tenant, worker, session uuid.UUID) ([]WorkerDelivery, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT d.delivery_id, d.action FROM v1_worker_delivery d
		JOIN v1_task_runtime r USING (task_id, task_inserted_at, retry_count, tenant_id, worker_id)
		JOIN "Worker" w ON w.id = d.worker_id AND w."tenantId" = d.tenant_id
		WHERE d.tenant_id = $1 AND d.worker_id = $2 AND w."lastListenerSessionId" = $3
			AND w."isActive" AND d.receipt_id IS NULL AND r.timeout_at > now()
		ORDER BY d.task_id LIMIT 32`, tenant, worker, session)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[WorkerDelivery])
}

func (w *workerRepository) ClaimWorkerDelivery(ctx context.Context, tenant, worker, session, delivery, receipt uuid.UUID) (bool, error) {
	// Compare against the live assignment as well as the receipt. A retry of a lost
	// RPC response uses the same receipt; a second copy of the start uses a new one.
	tag, err := w.pool.Exec(ctx, `
		UPDATE v1_worker_delivery d SET receipt_id = $5, action = NULL
		FROM v1_task_runtime r, "Worker" w
		WHERE d.tenant_id = $1 AND d.worker_id = $2 AND d.delivery_id = $4
			AND (r.task_id, r.task_inserted_at, r.retry_count, r.tenant_id, r.worker_id)
			  = (d.task_id, d.task_inserted_at, d.retry_count, d.tenant_id, d.worker_id)
			AND r.timeout_at > now()
			AND w.id = d.worker_id AND w."tenantId" = d.tenant_id
			AND (d.receipt_id = $5 OR (d.receipt_id IS NULL AND w."isActive" AND w."lastListenerSessionId" = $3))`,
		tenant, worker, session, delivery, receipt)
	return tag.RowsAffected() == 1, err
}

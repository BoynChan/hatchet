-- +goose Up
CREATE TABLE v1_worker_delivery (
    task_id bigint NOT NULL,
    task_inserted_at timestamptz NOT NULL,
    retry_count integer NOT NULL,
    tenant_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    delivery_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    receipt_id uuid,
    action bytea,
    PRIMARY KEY (task_id, task_inserted_at, retry_count),
    FOREIGN KEY (task_id, task_inserted_at, retry_count)
        REFERENCES v1_task_runtime (task_id, task_inserted_at, retry_count) ON DELETE CASCADE,
    CHECK ((receipt_id IS NULL) = (action IS NOT NULL))
);

CREATE INDEX v1_worker_delivery_pending_idx
    ON v1_worker_delivery (tenant_id, worker_id, task_id) WHERE receipt_id IS NULL;

-- +goose Down
DROP TABLE v1_worker_delivery;

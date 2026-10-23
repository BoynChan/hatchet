//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestOLAPReadPayloadsAndWorkflowRunStatus(t *testing.T) {
	basePool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	pool := createEnumAwarePool(t, basePool)
	repo := createOLAPRepositoryWithPayloadStore(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	require.NoError(t, repo.UpdateTablePartitions(ctx))

	f := seedReplayTask(t, ctx, repo, 42)

	// 1. Verify ReadWorkflowRunStatus returns initial QUEUED status and tenant ID.
	statusPopulator, err := repo.ReadWorkflowRunStatus(ctx, f.externalId)
	require.NoError(t, err)
	require.NotNil(t, statusPopulator)
	require.NotNil(t, statusPopulator.WorkflowRun)
	assert.Equal(t, f.tenantId, statusPopulator.WorkflowRun.TenantID)
	assert.Equal(t, f.externalId, statusPopulator.WorkflowRun.ExternalID)
	assert.Equal(t, sqlcv1.V1ReadableStatusOlapQUEUED, statusPopulator.WorkflowRun.ReadableStatus)
	assert.Equal(t, sqlcv1.V1RunKindTASK, statusPopulator.WorkflowRun.Kind)

	// 2. Verify ReadWorkflowRunStatus returns pgx.ErrNoRows for unknown external ID.
	_, err = repo.ReadWorkflowRunStatus(ctx, uuid.New())
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 3. Complete the task with an output payload and verify both ReadWorkflowRunStatus
	// and ReadPayload (with partition-pruning minInsertedAt/maxInsertedAt).
	batches := replayEventBatches(f)
	eventExternalIdToWorkflowRunId := map[uuid.UUID]uuid.UUID{f.externalId: f.externalId}
	_, locksNotAcquired, err := repo.CreateTaskEvents(ctx, f.tenantId, batches[0], eventExternalIdToWorkflowRunId, nil, nil)
	require.NoError(t, err)
	require.Empty(t, locksNotAcquired)

	statusPopulator, err = repo.ReadWorkflowRunStatus(ctx, f.externalId)
	require.NoError(t, err)
	assert.Equal(t, sqlcv1.V1ReadableStatusOlapCOMPLETED, statusPopulator.WorkflowRun.ReadableStatus)

	// 4. Verify ReadPayload with exact InsertedAt and batch readPayloads across timestamps.
	payloadID1 := uuid.New()
	payloadID2 := uuid.New()
	ts1 := pgtype.Timestamptz{Time: f.insertedAt.Time, Valid: true}
	ts2 := pgtype.Timestamptz{Time: f.insertedAt.Time.Add(5 * time.Second), Valid: true}

	err = repo.PutPayloads(ctx, nil, f.tenantId,
		StoreOLAPPayloadOpts{
			ExternalId: payloadID1,
			InsertedAt: ts1,
			Payload:    []byte(`{"step":"input"}`),
		},
		StoreOLAPPayloadOpts{
			ExternalId: payloadID2,
			InsertedAt: ts2,
			Payload:    []byte(`{"step":"output"}`),
		},
	)
	require.NoError(t, err)

	p1, err := repo.ReadPayload(ctx, f.tenantId, ReadOLAPPayloadOpts{
		ExternalId: payloadID1,
		InsertedAt: ts1,
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"step":"input"}`, string(p1))

	batchPayloads, err := repo.readPayloads(ctx, pool, f.tenantId,
		ReadOLAPPayloadOpts{ExternalId: payloadID1, InsertedAt: ts1},
		ReadOLAPPayloadOpts{ExternalId: payloadID2, InsertedAt: ts2},
	)
	require.NoError(t, err)
	assert.JSONEq(t, `{"step":"input"}`, string(batchPayloads[payloadID1]))
	assert.JSONEq(t, `{"step":"output"}`, string(batchPayloads[payloadID2]))

	// Reading with an out-of-range InsertedAt should prune out the partition/row.
	tsOther := pgtype.Timestamptz{Time: f.insertedAt.Time.Add(10 * time.Minute), Valid: true}
	pMissing, err := repo.ReadPayload(ctx, f.tenantId, ReadOLAPPayloadOpts{
		ExternalId: payloadID1,
		InsertedAt: tsOther,
	})
	require.NoError(t, err)
	assert.Nil(t, pMissing)
}

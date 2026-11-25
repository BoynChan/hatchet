//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/enums"
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
	assert.Equal(t, enums.V1ReadableStatusOlapQUEUED, statusPopulator.WorkflowRun.ReadableStatus)
	assert.Equal(t, enums.V1RunKindTASK, statusPopulator.WorkflowRun.Kind)

	// 2. Verify ReadWorkflowRunStatus returns pgx.ErrNoRows for unknown external ID.
	_, err = repo.ReadWorkflowRunStatus(ctx, uuid.New())
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 3. Complete the task and verify ReadWorkflowRunStatus transitions to COMPLETED.
	batches := replayEventBatches(f)
	eventExternalIdToWorkflowRunId := map[uuid.UUID]uuid.UUID{f.externalId: f.externalId}
	_, locksNotAcquired, err := repo.CreateTaskEvents(ctx, f.tenantId, batches[0], eventExternalIdToWorkflowRunId, nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, locksNotAcquired)

	statusPopulator, err = repo.ReadWorkflowRunStatus(ctx, f.externalId)
	require.NoError(t, err)
	assert.Equal(t, enums.V1ReadableStatusOlapCOMPLETED, statusPopulator.WorkflowRun.ReadableStatus)
}

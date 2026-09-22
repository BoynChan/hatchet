package v1

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
)

func TestCompletedTaskMessageRetainsReportingWorker(t *testing.T) {
	worker := uuid.New().String()
	msg, err := CompletedTaskMessage(uuid.New(), 1, pgtype.Timestamptz{}, uuid.New(), uuid.New(), 0, []byte(`{}`), worker)
	require.NoError(t, err)
	payloads := msgqueue.JSONConvert[CompletedTaskPayload](msg.Payloads)
	require.Len(t, payloads, 1)
	require.Equal(t, worker, payloads[0].WorkerId)
}

package dispatcher

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStreamSendFailureReason(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected dispatchFailureReason
	}{
		{name: "eof", err: io.EOF, expected: dispatchFailureStreamEOF},
		{name: "unavailable", err: status.Error(codes.Unavailable, "unavailable"), expected: dispatchFailureStreamUnavailable},
		{name: "canceled", err: status.Error(codes.Canceled, "canceled"), expected: dispatchFailureStreamCanceled},
		{name: "deadline", err: status.Error(codes.DeadlineExceeded, "deadline"), expected: dispatchFailureStreamDeadlineExceeded},
		{name: "other", err: errors.New("send failed"), expected: dispatchFailureStreamSend},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, streamSendFailureReason(test.err))
		})
	}
}

func TestWorkerSendErrorPreservesCauseAndReason(t *testing.T) {
	err := newWorkerSendError(dispatchFailureContextDone, context.DeadlineExceeded)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, dispatchFailureContextDone, workerSendFailureReason(err))
}

func TestWorkerLookupFailureReason(t *testing.T) {
	reason, failed := workerLookupFailureReason(ErrWorkerNotFound, 0)
	require.True(t, failed)
	assert.Equal(t, dispatchFailureWorkerNotFound, reason)

	reason, failed = workerLookupFailureReason(nil, 0)
	require.True(t, failed)
	assert.Equal(t, dispatchFailureNoActiveListenerSessions, reason)

	_, failed = workerLookupFailureReason(nil, 1)
	assert.False(t, failed)
}

func TestWorkerRegistryCanRetainAnEmptySessionSet(t *testing.T) {
	registry := &workers{}
	workerID := uuid.New()
	sessionID := uuid.New()
	registry.Add(workerID, sessionID, nil)
	registry.DeleteForSession(workerID, sessionID)

	sessions, err := registry.Get(workerID)

	require.NoError(t, err)
	assert.Empty(t, sessions)
}

func TestGetLocalWorkerIdsExcludesWorkersWithoutActiveSessions(t *testing.T) {
	d := &DispatcherImpl{workers: &workers{}}

	activeWorkerID := uuid.New()
	disconnectedWorkerID := uuid.New()
	reconnectedWorkerID := uuid.New()

	activeSessionID := uuid.New()
	disconnectedSessionID := uuid.New()
	oldSessionID := uuid.New()
	newSessionID := uuid.New()

	d.workers.Add(activeWorkerID, activeSessionID, &subscribedWorker{workerId: activeWorkerID})
	d.workers.Add(disconnectedWorkerID, disconnectedSessionID, &subscribedWorker{workerId: disconnectedWorkerID})
	d.workers.Add(reconnectedWorkerID, oldSessionID, &subscribedWorker{workerId: reconnectedWorkerID})
	d.workers.Add(reconnectedWorkerID, newSessionID, &subscribedWorker{workerId: reconnectedWorkerID})

	d.workers.DeleteForSession(disconnectedWorkerID, disconnectedSessionID)
	d.workers.DeleteForSession(reconnectedWorkerID, oldSessionID)

	localWorkerIDs := d.GetLocalWorkerIds()

	assert.Contains(t, localWorkerIDs, activeWorkerID)
	assert.Contains(t, localWorkerIDs, reconnectedWorkerID)
	assert.NotContains(t, localWorkerIDs, disconnectedWorkerID)

	d.workers.DeleteForSession(reconnectedWorkerID, newSessionID)

	localWorkerIDs = d.GetLocalWorkerIds()

	assert.Contains(t, localWorkerIDs, activeWorkerID)
	assert.NotContains(t, localWorkerIDs, reconnectedWorkerID)
	assert.NotContains(t, localWorkerIDs, disconnectedWorkerID)
}

func TestCombinedWorkerSendFailureReason(t *testing.T) {
	assert.Equal(
		t,
		dispatchFailureStreamEOF,
		combinedWorkerSendFailureReason([]dispatchFailureReason{
			dispatchFailureStreamEOF,
			dispatchFailureStreamEOF,
		}),
	)
	assert.Equal(
		t,
		dispatchFailureMultipleStreamErrors,
		combinedWorkerSendFailureReason([]dispatchFailureReason{
			dispatchFailureStreamEOF,
			dispatchFailureStreamUnavailable,
		}),
	)
	assert.Equal(t, dispatchFailureUnknown, combinedWorkerSendFailureReason(nil))
}

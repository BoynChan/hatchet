package dispatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	"github.com/hatchet-dev/hatchet/internal/services/shared/rpcstream"
)

type fakeActionStream struct {
	sent []*contracts.AssignedAction
	err  error
}

func (f *fakeActionStream) Send(action *contracts.AssignedAction) error {
	f.sent = append(f.sent, action)
	return f.err
}

func TestSubscribedWorkerStreamFailureRetainsDispatchReason(t *testing.T) {
	cause := status.Error(codes.Unavailable, "stream disconnected")
	stream := &fakeActionStream{err: cause}
	sender := rpcstream.NewSender[contracts.AssignedAction](context.Background(), stream)
	defer sender.Close()
	worker := newGRPCSubscribedWorker(sender, nil, uuid.New(), time.Second, nil)

	err := worker.sendToWorker(context.Background(), &contracts.AssignedAction{})

	require.ErrorIs(t, err, cause)
	require.Equal(t, dispatchFailureStreamUnavailable, workerSendFailureReason(err))
}

// Once the Listen handler has returned, its stream must never be written to again: the HTTP/2
// server panics on a write after the handler is done. A send that loses the race with the
// handler returning has to come back as an error instead.
func TestSubscribedWorker_SendAfterStreamCloseReturnsError(t *testing.T) {
	stream := &fakeActionStream{}
	sender := rpcstream.NewSender[contracts.AssignedAction](context.Background(), stream)
	worker := newGRPCSubscribedWorker(sender, nil, uuid.New(), time.Second, nil)

	if err := worker.sendToWorker(context.Background(), &contracts.AssignedAction{}); err != nil {
		t.Fatalf("expected send on an open stream to succeed, got %v", err)
	}

	sender.Close()

	err := worker.sendToWorker(context.Background(), &contracts.AssignedAction{})

	if !errors.Is(err, rpcstream.ErrClosed) {
		t.Fatalf("expected ErrClosed after the stream is closed, got %v", err)
	}

	if len(stream.sent) != 1 {
		t.Fatalf("expected exactly one action to reach the stream, got %d", len(stream.sent))
	}
}

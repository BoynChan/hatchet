package dispatcher

import (
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dispatchFailureReason string

const (
	dispatchFailureWorkerNotFound           dispatchFailureReason = "worker_not_found"
	dispatchFailureNoActiveListenerSessions dispatchFailureReason = "no_active_listener_sessions"
	dispatchFailureActionEncode             dispatchFailureReason = "action_encode_error"
	dispatchFailureSendLockTimeout          dispatchFailureReason = "send_lock_timeout"
	dispatchFailureContextDone              dispatchFailureReason = "context_done"
	dispatchFailureStreamEOF                dispatchFailureReason = "stream_eof"
	dispatchFailureStreamUnavailable        dispatchFailureReason = "stream_unavailable"
	dispatchFailureStreamCanceled           dispatchFailureReason = "stream_canceled"
	dispatchFailureStreamDeadlineExceeded   dispatchFailureReason = "stream_deadline_exceeded"
	dispatchFailureStreamSend               dispatchFailureReason = "stream_send_error"
	dispatchFailureMultipleStreamErrors     dispatchFailureReason = "multiple_stream_errors"
	dispatchFailureUnknown                  dispatchFailureReason = "unknown"
)

type workerSendError struct {
	reason dispatchFailureReason
	cause  error
}

func (e *workerSendError) Error() string {
	return e.cause.Error()
}

func (e *workerSendError) Unwrap() error {
	return e.cause
}

func newWorkerSendError(reason dispatchFailureReason, cause error) error {
	return &workerSendError{reason: reason, cause: cause}
}

func streamSendFailureReason(err error) dispatchFailureReason {
	if errors.Is(err, io.EOF) {
		return dispatchFailureStreamEOF
	}

	switch status.Code(err) {
	case codes.Unavailable:
		return dispatchFailureStreamUnavailable
	case codes.Canceled:
		return dispatchFailureStreamCanceled
	case codes.DeadlineExceeded:
		return dispatchFailureStreamDeadlineExceeded
	default:
		return dispatchFailureStreamSend
	}
}

func workerSendFailureReason(err error) dispatchFailureReason {
	var sendErr *workerSendError
	if errors.As(err, &sendErr) {
		return sendErr.reason
	}

	return dispatchFailureUnknown
}

func workerLookupFailureReason(err error, sessionCount int) (dispatchFailureReason, bool) {
	if errors.Is(err, ErrWorkerNotFound) {
		return dispatchFailureWorkerNotFound, true
	}

	if err == nil && sessionCount == 0 {
		return dispatchFailureNoActiveListenerSessions, true
	}

	return "", false
}

func combinedWorkerSendFailureReason(reasons []dispatchFailureReason) dispatchFailureReason {
	if len(reasons) == 0 {
		return dispatchFailureUnknown
	}

	first := reasons[0]
	for _, reason := range reasons[1:] {
		if reason != first {
			return dispatchFailureMultipleStreamErrors
		}
	}

	return first
}

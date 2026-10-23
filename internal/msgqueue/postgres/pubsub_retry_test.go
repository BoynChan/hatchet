package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	"github.com/hatchet-dev/hatchet/pkg/repository"
)

type retryRepository struct {
	repository.MessageQueueRepository
	notifyErrors []error
	notifyCalls  int
	bindCalls    int
	bindError    error
}

func (r *retryRepository) Notify(context.Context, string, string) error {
	i := r.notifyCalls
	r.notifyCalls++
	return r.notifyErrors[i]
}

func (r *retryRepository) BindQueue(context.Context, string, bool, bool, bool, *string) error {
	r.bindCalls++
	if r.bindCalls > 1 {
		return r.bindError
	}
	return nil
}

func TestPostgresPubSubRebindRetry(t *testing.T) {
	missingQueue := &pgconn.PgError{Code: "23503", ConstraintName: "MessageQueueItem_queueId_fkey"}
	otherConstraint := &pgconn.PgError{Code: "23503", ConstraintName: "another_fkey"}
	tooLong := &pgconn.PgError{Code: "22023"}
	connectionFailure := errors.New("connection closed")
	for _, tc := range []struct {
		name         string
		notifyErrors []error
		bindError    error
		wantError    error
		wantBinds    int
	}{
		{"success", []error{nil}, nil, nil, 1},
		{"missing queue", []error{missingQueue, nil}, nil, nil, 2},
		{"wrapped missing queue", []error{fmt.Errorf("copy: %w", missingQueue), nil}, nil, nil, 2},
		{"only one retry", []error{missingQueue, missingQueue}, nil, missingQueue, 2},
		{"rebind fails", []error{missingQueue}, connectionFailure, connectionFailure, 2},
		{"other constraint", []error{otherConstraint}, nil, otherConstraint, 1},
		{"notify limit", []error{tooLong}, nil, tooLong, 1},
		{"ambiguous delivery", []error{connectionFailure}, nil, connectionFailure, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &retryRepository{notifyErrors: tc.notifyErrors, bindError: tc.bindError}
			cleanup, ps, err := NewPubSub(repo)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cleanup()) })
			topic := msgqueue.TenantTopic(uuid.New())
			err = ps.Pub(context.Background(), topic, &msgqueue.Message{Payloads: [][]byte{[]byte(`{}`)}})
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantError)
			}
			require.Equal(t, len(tc.notifyErrors), repo.notifyCalls)
			require.Equal(t, tc.wantBinds, repo.bindCalls)
		})
	}
}

package dispatcher

import (
	"context"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func (worker *subscribedWorker) sendRecoverableStart(ctx context.Context, tenant uuid.UUID, task *sqlcv1.V1Task, action *contracts.AssignedAction) error {
	data, err := proto.Marshal(action)
	if err != nil {
		return err
	}
	id, err := worker.deliveries.SaveWorkerDelivery(ctx, tenant, worker.workerId, task, data)
	if err != nil || id == uuid.Nil {
		return err
	}
	action.DeliveryId = id.String()
	action.ListenerSessionId = worker.sessionID.String()
	// The committed delivery now belongs to this worker. A transport error can
	// race a successful claim; only the runtime owner may retire the attempt.
	_ = worker.sendToWorker(ctx, action)
	return nil
}

func (worker *subscribedWorker) recoverDeliveries(ctx context.Context, tenant uuid.UUID) error {
	// Bound recovery writes so a stalled stream cannot hold ListenV2 open on shutdown.
	ctx, cancel := context.WithTimeout(ctx, HeartbeatInterval)
	defer cancel()
	items, err := worker.deliveries.PendingWorkerDeliveries(ctx, tenant, worker.workerId, worker.sessionID)
	if err != nil {
		return err
	}
	for _, item := range items {
		action := &contracts.AssignedAction{}
		if err := proto.Unmarshal(item.Action, action); err != nil {
			return err
		}
		action.DeliveryId = item.ID.String()
		action.ListenerSessionId = worker.sessionID.String()
		if err := worker.sendToWorker(ctx, action); err != nil {
			return err
		}
	}
	return nil
}

func (s *DispatcherImpl) ClaimDelivery(ctx context.Context, req *contracts.ClaimDeliveryRequest) (*contracts.ClaimDeliveryResponse, error) {
	worker, err := uuid.Parse(req.WorkerId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	session, err := uuid.Parse(req.ListenerSessionId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	delivery, err := uuid.Parse(req.DeliveryId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	receipt, err := uuid.Parse(req.ReceiptId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	tenant := ctx.Value("tenant").(*sqlcv1.Tenant)
	accepted, err := s.repov1.Workers().ClaimWorkerDelivery(ctx, tenant.ID, worker, session, delivery, receipt)
	if err != nil {
		return nil, err
	}
	return &contracts.ClaimDeliveryResponse{Accepted: accepted}, nil
}

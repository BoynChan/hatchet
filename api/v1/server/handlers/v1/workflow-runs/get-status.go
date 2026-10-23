package workflowruns

import (
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/labstack/echo/v4"

	"github.com/hatchet-dev/hatchet/api/v1/server/oas/gen"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func (t *V1WorkflowRunsService) V1WorkflowRunGetStatus(ctx echo.Context, request gen.V1WorkflowRunGetStatusRequestObject) (gen.V1WorkflowRunGetStatusResponseObject, error) {
	var maybeWorkflowRun *v1.V1WorkflowRunPopulator

	if v, ok := ctx.Get("v1-workflow-run-status").(*v1.V1WorkflowRunPopulator); ok && v != nil {
		maybeWorkflowRun = v
	} else if v, ok := ctx.Get("v1-workflow-run").(*v1.V1WorkflowRunPopulator); ok && v != nil {
		maybeWorkflowRun = v
	} else {
		var err error
		maybeWorkflowRun, err = t.config.V1.OLAP().ReadWorkflowRunStatus(ctx.Request().Context(), request.V1WorkflowRun)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, echo.NewHTTPError(http.StatusNotFound, "not found")
			}
			return nil, err
		}
	}

	status := gen.V1TaskStatus(maybeWorkflowRun.WorkflowRun.ReadableStatus)
	if maybeWorkflowRun.WorkflowRun.ReadableStatus == sqlcv1.V1ReadableStatusOlapEVICTED {
		status = gen.V1TaskStatusRUNNING
	}

	return gen.V1WorkflowRunGetStatus200JSONResponse(status), nil
}

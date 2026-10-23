//go:build !e2e && !load && !rampup && !integration

package workflowruns

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/api/v1/server/oas/gen"
	v1 "github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestV1WorkflowRunGetStatus_UsesLightweightPopulatorContext(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	runID := uuid.New()
	c.Set("v1-workflow-run-status", &v1.V1WorkflowRunPopulator{
		WorkflowRun: &v1.WorkflowRunData{
			ExternalID:     runID,
			ReadableStatus: sqlcv1.V1ReadableStatusOlapCOMPLETED,
		},
	})

	svc := &V1WorkflowRunsService{}
	resp, err := svc.V1WorkflowRunGetStatus(c, gen.V1WorkflowRunGetStatusRequestObject{
		V1WorkflowRun: runID,
	})
	require.NoError(t, err)
	assert.Equal(t, gen.V1WorkflowRunGetStatus200JSONResponse(gen.V1TaskStatusCOMPLETED), resp)
}

func TestV1WorkflowRunGetStatus_MapsEvictedToRunning(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	runID := uuid.New()
	c.Set("v1-workflow-run-status", &v1.V1WorkflowRunPopulator{
		WorkflowRun: &v1.WorkflowRunData{
			ExternalID:     runID,
			ReadableStatus: sqlcv1.V1ReadableStatusOlapEVICTED,
		},
	})

	svc := &V1WorkflowRunsService{}
	resp, err := svc.V1WorkflowRunGetStatus(c, gen.V1WorkflowRunGetStatusRequestObject{
		V1WorkflowRun: runID,
	})
	require.NoError(t, err)
	assert.Equal(t, gen.V1WorkflowRunGetStatus200JSONResponse(gen.V1TaskStatusRUNNING), resp)
}

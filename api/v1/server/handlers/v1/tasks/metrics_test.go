//go:build !e2e && !load && !rampup && !integration

package tasks

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/api/v1/server/oas/gen"
)

func TestV1TaskListStatusMetrics_ReturnsZeroCountsWithoutQueryingDB(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// TasksService with nil config proves no database or OLAP repository method is called.
	svc := &TasksService{}
	resp, err := svc.V1TaskListStatusMetrics(c, gen.V1TaskListStatusMetricsRequestObject{
		Tenant: uuid.New(),
	})
	require.NoError(t, err)

	metricsResp, ok := resp.(gen.V1TaskListStatusMetrics200JSONResponse)
	require.True(t, ok)
	require.Len(t, metricsResp, 5)
	for _, m := range metricsResp {
		assert.Equal(t, 0, m.Count)
		if m.Status == gen.V1TaskStatusRUNNING {
			require.NotNil(t, m.RunningDetailCount)
			assert.Equal(t, 0, m.RunningDetailCount.Evicted)
			assert.Equal(t, 0, m.RunningDetailCount.OnWorker)
		}
	}
}

func TestV1TaskGetPointMetrics_ReturnsZeroBucketsWithoutQueryingDB(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	createdAfter := time.Date(2026, 10, 23, 14, 0, 0, 0, time.UTC)
	finishedBefore := time.Date(2026, 10, 23, 14, 10, 0, 0, time.UTC)

	// TasksService with nil config proves no database or OLAP repository method is called.
	svc := &TasksService{}
	resp, err := svc.V1TaskGetPointMetrics(c, gen.V1TaskGetPointMetricsRequestObject{
		Tenant: uuid.New(),
		Params: gen.V1TaskGetPointMetricsParams{
			CreatedAfter:   &createdAfter,
			FinishedBefore: &finishedBefore,
		},
	})
	require.NoError(t, err)

	pointResp, ok := resp.(gen.V1TaskGetPointMetrics200JSONResponse)
	require.True(t, ok)
	require.NotNil(t, pointResp.Results)
	require.Len(t, *pointResp.Results, 11)
	for _, pt := range *pointResp.Results {
		assert.Equal(t, 0, pt.FAILED)
		assert.Equal(t, 0, pt.SUCCEEDED)
	}
}

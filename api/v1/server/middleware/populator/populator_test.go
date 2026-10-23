//go:build !e2e && !load && !rampup && !integration

package populator

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"

	"github.com/hatchet-dev/hatchet/api/v1/server/middleware"
	"github.com/hatchet-dev/hatchet/api/v1/server/oas/gen"
	"github.com/hatchet-dev/hatchet/pkg/config/server"
)

type oneToManyResource struct {
	ID       string `json:"id"`
	ParentID string `json:"parent_id"`
}

func oneToManyResourceGetter(config *server.ServerConfig, parentId, id string) (interface{}, string, error) {
	if parentId == "" {
		parentId := uuid.NewString()

		return &oneToManyResource{
			ID:       id,
			ParentID: parentId,
		}, parentId, nil
	}

	return &oneToManyResource{
		ID:       id,
		ParentID: parentId,
	}, parentId, nil
}

type topLevelResource struct {
	ID string `json:"id"`
}

func topLevelResourceGetter(config *server.ServerConfig, parentId, id string) (interface{}, string, error) {
	return &topLevelResource{
		ID: id,
	}, "", nil
}

func invalidParentGetter(config *server.ServerConfig, parentId, id string) (interface{}, string, error) {
	newUuid := uuid.NewString()

	return &oneToManyResource{
		ID:       id,
		ParentID: newUuid,
	}, newUuid, nil
}

func TestPopulatorMiddleware(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Mock RouteInfo
	routeInfo := &middleware.RouteInfo{
		Resources: []string{"resource1", "resource2"},
	}

	resource2Id := uuid.New().String()

	// Setting params for the context
	c.SetParamNames("resource2")
	c.SetParamValues(resource2Id)

	// Creating Populator with mock getter function
	populator := NewPopulator(&server.ServerConfig{})

	populator.RegisterGetter("resource1", topLevelResourceGetter)
	populator.RegisterGetter("resource2", oneToManyResourceGetter)

	// Using the Populator middleware
	middlewareFunc := populator.Middleware(routeInfo)
	err := middlewareFunc(c)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, c.Get("resource1"))
	assert.NotNil(t, c.Get("resource2"))
}

func TestPopulatorMiddlewareParentDisagreement(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	// Mock RouteInfo
	routeInfo := &middleware.RouteInfo{
		Resources: []string{"resource1", "resource2"},
	}

	resource1Id := uuid.New().String()
	resource2Id := uuid.New().String()

	// Setting params for the context - NOTE: we need to set both params at once
	c.SetParamNames("resource1", "resource2")
	c.SetParamValues(resource1Id, resource2Id)

	// Creating Populator with mock getter function
	populator := NewPopulator(&server.ServerConfig{})

	populator.RegisterGetter("resource1", topLevelResourceGetter)
	populator.RegisterGetter("resource2", invalidParentGetter)

	// Using the Populator middleware
	middlewareFunc := populator.Middleware(routeInfo)
	err := middlewareFunc(c)

	// Assertions
	assert.ErrorContains(t, err, "not authorized to access this resource")
}

func TestPopulatorMiddlewareWorkflowRunGetStatusUsesLightweightGetter(t *testing.T) {
	spec, err := gen.GetSwagger()
	assert.NoError(t, err)

	statusPathItem := spec.Paths.Find("/api/v1/stable/workflow-runs/{v1-workflow-run}/status")
	assert.NotNil(t, statusPathItem)
	assert.NotNil(t, statusPathItem.Get)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stable/workflow-runs/00000000-0000-0000-0000-000000000001/status", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	routeInfo := &middleware.RouteInfo{
		OperationID: statusPathItem.Get.OperationID,
		Resources:   []string{"tenant", "v1-workflow-run"},
	}

	workflowRunId := uuid.New().String()
	tenantId := uuid.New().String()

	c.SetParamNames("v1-workflow-run")
	c.SetParamValues(workflowRunId)

	populator := NewPopulator(&server.ServerConfig{})

	heavyGetterCalled := false
	lightweightGetterCalled := false

	populator.RegisterGetter("tenant", topLevelResourceGetter)
	populator.RegisterGetter("v1-workflow-run", func(config *server.ServerConfig, parentId, id string) (interface{}, string, error) {
		heavyGetterCalled = true
		return &oneToManyResource{ID: id, ParentID: tenantId}, tenantId, nil
	})
	populator.RegisterGetter("v1-workflow-run-status", func(config *server.ServerConfig, parentId, id string) (interface{}, string, error) {
		lightweightGetterCalled = true
		return &oneToManyResource{ID: id, ParentID: tenantId}, tenantId, nil
	})

	err = populator.Middleware(routeInfo)(c)
	assert.NoError(t, err)
	assert.False(t, heavyGetterCalled, "v1-workflow-run getter should not be called for get-status")
	assert.True(t, lightweightGetterCalled, "v1-workflow-run-status getter should be called for get-status")
	assert.NotNil(t, c.Get("tenant"))
	assert.NotNil(t, c.Get("v1-workflow-run-status"))
}

package openapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/stretchr/testify/assert"
)

func TestGetSpec(t *testing.T) {
	assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		o, err := New(Config{
			Title:       "testing",
			Version:     "v0.0.0",
			Description: "testing openapi schema",
			Port:        8080,
		})
		assert.NoError(t, err)
		oapi := o.(*openAPIServer)
		assert.NoError(t, oapi.registerRoutes(ctx, db))
		s := httptest.NewServer(oapi.router)
		defer s.Close()
		{
			req, err := http.NewRequest(http.MethodGet, s.URL+"/openapi.yaml", nil)
			assert.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			assert.NoError(t, err)
			defer resp.Body.Close()
			bits, _ := io.ReadAll(resp.Body)
			assert.Equal(t, 200, resp.StatusCode, string(bits))
		}
		{
			req, err := http.NewRequest(http.MethodGet, s.URL+"/openapi.json", nil)
			assert.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			assert.NoError(t, err)
			defer resp.Body.Close()
			bits, _ := io.ReadAll(resp.Body)
			assert.Equal(t, 200, resp.StatusCode, string(bits))
		}

	}))
}

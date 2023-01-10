package openapi

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/extentions/openapi/testdata"
	"github.com/autom8ter/myjson/testutil"
	"github.com/stretchr/testify/assert"
)

func TestGetAccountSchema(t *testing.T) {
	assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		oapi, err := New(Config{
			Title:       "testing",
			Version:     "v0.0.0",
			Description: "testing openapi schema",
			Port:        8080,
		})
		assert.NoError(t, err)
		assert.NoError(t, oapi.RegisterRoutes(ctx, db))
		s := httptest.NewServer(oapi.router)
		defer s.Close()
		client, err := testdata.NewClientWithResponses(s.URL)
		assert.NoError(t, err)

		account, err := client.GetAccountSchemaWithResponse(ctx)
		assert.Equal(t, 200, account.StatusCode())
	}))
}

func TestGetSchemas(t *testing.T) {
	assert.NoError(t, testutil.TestDB(func(ctx context.Context, db myjson.Database) {
		oapi, err := New(Config{
			Title:       "testing",
			Version:     "v0.0.0",
			Description: "testing openapi schema",
			Port:        8080,
		})
		assert.NoError(t, err)
		assert.NoError(t, oapi.RegisterRoutes(ctx, db))
		s := httptest.NewServer(oapi.router)
		defer s.Close()
		client, err := testdata.NewClient(s.URL)
		assert.NoError(t, err)

		schemas, err := client.GetSchemas(ctx)
		bits, _ := io.ReadAll(schemas.Body)
		assert.Equal(t, 200, schemas.StatusCode, string(bits))
	}))
}

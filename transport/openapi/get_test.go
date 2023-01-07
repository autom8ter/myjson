package openapi

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/autom8ter/myjson/transport/openapi/testdata"
	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
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
		client, err := testdata.NewClientWithResponses(s.URL)
		assert.NoError(t, err)

		account, err := client.GetAccountWithResponse(ctx, "0")
		assert.NoError(t, err)
		assert.NotEmpty(t, account.JSON200)
		assert.Equal(t, 200, account.StatusCode(), string(account.Body))
	}))
}

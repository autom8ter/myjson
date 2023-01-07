package openapi

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/autom8ter/myjson/transport/openapi/testdata"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
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
		client, err := testdata.NewClient(s.URL)
		assert.NoError(t, err)

		results, err := client.SetAccount(ctx, "0", testdata.SetAccountJSONRequestBody{
			Id:   "0",
			Name: gofakeit.Company(),
		})
		bits, _ := io.ReadAll(results.Body)
		assert.Equal(t, 200, results.StatusCode, string(bits))
	}))
}

package openapi

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/extentions/openapi/testdata"
	"github.com/autom8ter/myjson/testutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
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
		results, err := client.QueryAccountWithResponse(ctx, &testdata.QueryAccountParams{Explain: lo.ToPtr(true)}, testdata.QueryAccountJSONRequestBody{
			Select: &[]testdata.Select{
				{
					Field: "*",
				},
			},
			Limit: lo.ToPtr(1),
		})
		assert.Equal(t, 200, results.StatusCode())
		assert.Equal(t, "0", results.JSON200.Documents[0]["_id"])
	}))
}

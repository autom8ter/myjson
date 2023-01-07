package openapi

import (
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/autom8ter/myjson/transport/openapi/testdata"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
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
		results, err := client.QueryAccount(ctx, testdata.QueryAccountJSONRequestBody{
			GroupBy: nil,
			Limit:   lo.ToPtr(1),
			OrderBy: nil,
			Page:    nil,
			Select:  nil,
			Where:   nil,
		})
		assert.Equal(t, 200, results.StatusCode)
		bits, _ := io.ReadAll(results.Body)
		assert.NoError(t, err)
		resp, _ := myjson.NewDocumentFromBytes(bits)
		assert.Equal(t, "0", resp.Get("documents.0._id"))
	}))
}

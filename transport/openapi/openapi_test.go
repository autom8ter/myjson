package openapi

import (
	"context"
	"fmt"
	"testing"

	_ "embed"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

var (
	//go:embed testdata/openapi.yaml
	expectedSchema string
)

func TestOpenAPI(t *testing.T) {
	t.Run("getSpec", func(t *testing.T) {
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
			//f, _ := os.Create("testdata/openapi.yaml")
			//defer f.Close()
			//f.Write(oapi.spec)
			assert.YAMLEq(t, expectedSchema, string(oapi.spec))
		}))
	})
	t.Run("walk router", func(t *testing.T) {
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
			assert.NoError(t, oapi.router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
				fmt.Println(route.GetName())
				return nil
			}))

		}))
	})
}

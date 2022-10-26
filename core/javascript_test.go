package core_test

import (
	"context"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"testing"
)

var sumFunction = core.Javascript(`
function sum(input) {
	let output = {}
	output.sum = input.a * input.b;
    return output
}
`)

var countFunction = core.Javascript(`
function count(input) {
	let output = {}
	output.count = input.length
    return input.length
}
`)

var getWareFunction = core.Javascript(`
function transform(input) {
	let output = {};
	output.transformed = true;
    return output
}
`)

func Test(t *testing.T) {
	t.Run("get function names", func(t *testing.T) {
		sumName := sumFunction.FunctionName()
		assert.Equal(t, "sum", sumName)
		countName := countFunction.FunctionName()
		assert.Equal(t, "count", countName)
	})
	t.Run("sum", func(t *testing.T) {
		fn, err := sumFunction.Parse()
		assert.Nil(t, err)
		output, err := fn(map[string]any{
			"a": 4,
			"b": 7,
		})
		assert.EqualValues(t, 28, cast.ToStringMap(output)["sum"])
	})
	t.Run("count", func(t *testing.T) {
		fn, err := countFunction.Parse()
		assert.Nil(t, err)
		output, err := fn([]string{"1", "2", "3"})
		assert.Nil(t, err)
		assert.EqualValues(t, 3, output)
	})

	t.Run("getWare", func(t *testing.T) {
		fn, err := getWareFunction.Parse()
		assert.Nil(t, err)
		doc, err := fn.GetWare()(func(ctx context.Context, collection *core.Collection, id string) (*core.Document, error) {
			return testutil.NewUserDoc(), nil
		})(context.Background(), testutil.UserCollection, "1")
		assert.Nil(t, err)
		assert.Equal(t, true, doc.Get("transformed"))
	})
}

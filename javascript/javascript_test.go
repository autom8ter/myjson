package javascript_test

import (
	"github.com/autom8ter/wolverine/javascript"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"testing"
)

var sumFunction = javascript.Javascript(`
function sum(input) {
	let output = {}
	output.sum = input.a * input.b;
    return output
}
`)

var countFunction = javascript.Javascript(`
function count(input) {
	let output = {}
	output.count = input.length
    return input.length
}
`)

var getWareFunction = javascript.Javascript(`
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

}

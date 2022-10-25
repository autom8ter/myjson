package javascript_test

import (
	"github.com/autom8ter/wolverine/javascript"
	"github.com/stretchr/testify/assert"
	"testing"
)

var script = `
function sum(input) {
	let output = {}
	output.sum = input.a * input.b;
    return output
}
`

func Test(t *testing.T) {
	t.Run("exec", func(t *testing.T) {
		f := javascript.NewFunction([]string{"sum"}, script)
		f, err := f.Compile()
		assert.Nil(t, err)
		output, err := f.Exec("sum", map[string]any{
			"a": 4,
			"b": 7,
		})
		assert.Nil(t, err)
		assert.EqualValues(t, 28, output.(map[string]any)["sum"])
	})
}

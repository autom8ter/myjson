package javascript

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var sumFunction = `
function sum(input) {
	let output = {}
	output.sum = input.a * input.b;
    return output
}
`

var countFunction = `
function count(input) {
	let output = {}
	output.count = input.length()
    return output
}
`

func TestGetFunctionNames(t *testing.T) {
	t.Run("get function names", func(t *testing.T) {
		sumName := getFunctionName(sumFunction)
		assert.Equal(t, "sum", sumName)
		countName := getFunctionName(countFunction)
		assert.Equal(t, "count", countName)
	})
}

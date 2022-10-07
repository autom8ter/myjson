package wolverine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReducers(t *testing.T) {
	var documents []*Document
	for i := 0; i < 5; i++ {
		doc := NewDocument()
		doc.Set("value", float64(i))
		t.Log(doc.String())
		documents = append(documents, doc)
	}
	t.Run("count", func(t *testing.T) {
		result, err := countReducer()("value", documents)
		assert.Nil(t, err)
		metric := result.GetFloat("value.count")
		assert.NotNil(t, metric)
		assert.Equal(t, float64(5), metric)
	})
	t.Run("avg", func(t *testing.T) {
		result, err := avgReducer()("value", documents)
		assert.Nil(t, err)
		metric := result.GetFloat("value.avg")
		assert.NotNil(t, metric)
		assert.Equal(t, float64(2), metric)
	})
	t.Run("sum", func(t *testing.T) {
		result, err := sumReducer()("value", documents)
		assert.Nil(t, err)
		metric := result.GetFloat("value.sum")
		assert.NotNil(t, metric)
		assert.Equal(t, float64(10), metric)
	})
	t.Run("min", func(t *testing.T) {
		result, err := minReducer()("value", documents)
		assert.Nil(t, err)
		metric := result.GetFloat("value.min")
		assert.NotNil(t, metric)
		assert.Equal(t, float64(0), metric)
	})
	t.Run("max", func(t *testing.T) {
		result, err := maxReducer()("value", documents)
		assert.Nil(t, err)
		metric := result.GetFloat("value.max")
		assert.NotNil(t, metric)
		assert.Equal(t, float64(4), metric)
	})
}

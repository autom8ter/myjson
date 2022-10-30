package wolverine

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAggregate(t *testing.T) {
	t.Run("sum age", func(t *testing.T) {
		var expected = float64(0)
		var docs Documents
		for i := 0; i < 5; i++ {
			u := NewUserDoc()
			expected += u.GetFloat("age")
			docs = append(docs, u)
		}
		reduced, err := docs.Aggregate(context.Background(), []Aggregate{
			{
				Field:    "age",
				Function: "sum",
				Alias:    "age_sum",
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, expected, reduced.GetFloat("age_sum"))
	})
}

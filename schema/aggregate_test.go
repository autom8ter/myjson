package schema_test

import (
	"context"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestApplyReducers(t *testing.T) {
	t.Run("sum age", func(t *testing.T) {
		var expected = float64(0)
		var docs []*schema.Document
		for i := 0; i < 5; i++ {
			u := testutil.NewUserDoc()
			expected += u.GetFloat("age")
			docs = append(docs, u)
		}
		reduced, err := schema.ApplyReducers(context.Background(), schema.AggregateQuery{
			Aggregates: []schema.Aggregate{
				{
					Field:    "age",
					Function: "sum",
					Alias:    "age_sum",
				},
			},
		}, docs)

		assert.Nil(t, err)
		assert.Equal(t, expected, reduced.GetFloat("age_sum"))
	})
}

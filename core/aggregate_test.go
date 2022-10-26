package core_test

import (
	"context"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestApplyReducers(t *testing.T) {
	t.Run("sum age", func(t *testing.T) {
		var expected = float64(0)
		var docs []*core.Document
		for i := 0; i < 5; i++ {
			u := testutil.NewUserDoc()
			expected += u.GetFloat("age")
			docs = append(docs, u)
		}
		reduced, err := core.ApplyReducers(context.Background(), core.AggregateQuery{
			Aggregates: []core.Aggregate{
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

package schema_test

import (
	"context"
	"fmt"
	"github.com/autom8ter/wolverine/internal/testutil"
	"github.com/autom8ter/wolverine/schema"
	"github.com/reactivex/rxgo/v2"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAggregate(t *testing.T) {
	t.Run("pipe", func(t *testing.T) {
		totalRecords := 100000
		channel := make(chan rxgo.Item, totalRecords)
		go func() {
			for i := 0; i < 100000; i++ {
				channel <- rxgo.Of(testutil.NewUserDoc())
			}
			close(channel)
			fmt.Println("input channel closed")
		}()

		a := schema.AggregateQuery{
			GroupBy: []string{"account_id"},
			Aggregates: []schema.Aggregate{
				{
					Field:    "age",
					Function: "max",
					Alias:    "max_age",
				},
				//{
				//	Field:    "age",
				//	Function: "min",
				//	Alias:    "min_age",
				//},
			},
			Where: []schema.Where{
				{
					Field: "account_id",
					Op:    schema.Gt,
					Value: 50,
				},
			},
			OrderBy: schema.OrderBy{
				Field:     "account_id",
				Direction: schema.DESC,
			},
		}
		now := time.Now()
		observable, err := a.Observe(context.Background(), channel, true)
		assert.Nil(t, err)
		i := 0
		<-observable.ForEach(func(o interface{}) {
			i++
			t.Logf("%s", o)
			// t.Log(i.(*Document).String())
		}, func(err error) {
			t.Fatal(err)
		}, func() {

		})
		t.Log(i, float64(time.Since(now).Milliseconds())/float64(totalRecords))
	})
}

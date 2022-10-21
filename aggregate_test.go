package wolverine

import (
	"context"
	"fmt"
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
				channel <- rxgo.Of(newUserDoc())
			}
			close(channel)
			fmt.Println("input channel closed")
		}()

		a := AggregateQuery{
			GroupBy: []string{"account_id"},
			Aggregates: []Aggregate{
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
			Where: []Where{
				{
					Field: "account_id",
					Op:    Gt,
					Value: 50,
				},
			},
			OrderBy: OrderBy{
				Field:     "account_id",
				Direction: DESC,
			},
		}
		now := time.Now()
		observable, err := a.pipe(context.Background(), channel, true)
		assert.Nil(t, err)
		i := 0
		<-observable.ForEach(func(o interface{}) {
			i++
			// t.Log(i.(*Document).String())
		}, func(err error) {
			t.Fatal(err)
		}, func() {

		})
		t.Log(i, float64(time.Since(now).Milliseconds())/float64(totalRecords))
	})
}

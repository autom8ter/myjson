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
		channel := make(chan rxgo.Item, 100000)
		go func() {
			for i := 0; i < 100000; i++ {
				channel <- rxgo.Of(newUserDoc())
			}
			close(channel)
			fmt.Println("input channel closed")
		}()

		a := AggregateQuery{
			GroupBy: []string{"account_id", "gender"},
			Aggregates: []Aggregate{
				{
					Field:    "age",
					Function: "max",
					Alias:    "max_age",
				},
				{
					Field:    "age",
					Function: "min",
					Alias:    "min_age",
				},
			},
			Where: []Where{
				{
					Field: "account_id",
					Op:    Gt,
					Value: 50,
				},
			},
			OrderBy: OrderBy{},
		}
		now := time.Now()
		observable, err := a.pipe(context.Background(), channel, false)
		assert.Nil(t, err)
		<-observable.ForEach(func(i interface{}) {
			t.Log(i.(*Document).String())
		}, func(err error) {
			t.Fatal(err)
		}, func() {

		})
		t.Log(time.Since(now).Milliseconds())
	})
}

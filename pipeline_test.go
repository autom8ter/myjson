package wolverine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/reactivex/rxgo/v2"
)

func TestPipeline(t *testing.T) {
	var usrs []*Document
	for i := 0; i < 100; i++ {
		usrs = append(usrs, newUserDoc())
	}

	t.Run("group by", func(t *testing.T) {
		ch := make(chan rxgo.Item)
		go func() {
			for _, u := range usrs {
				ch <- rxgo.Of(u)
				fmt.Println(u.GetID())
			}
			close(ch)
		}()

		options := pipelineOpts{
			selectFields:  nil,
			groupByFields: []string{"account_id"},
			aggregates: []Aggregate{
				{
					Function: AggregateCount,
					Field:    "gender",
				},
			},
			wheres:  nil,
			orderBy: OrderBy{},
			page:    0,
			limit:   0,
		}
		//
		result := queryStream(context.Background(), options, ch)
		bits, _ := json.MarshalIndent(result, "", "    ")
		t.Log(string(bits))
	})
}

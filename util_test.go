package wolverine

import (
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestUtil(t *testing.T) {
	t.Run("prune1", func(t *testing.T) {
		var docs []*Document
		for i := 0; i < 100; i++ {
			docs = append(docs, newUserDoc())
		}
		{
			pruned, hasNext := prunePage(0, 3, docs)
			assert.Equal(t, 3, len(pruned))
			assert.EqualValues(t, docs[0], pruned[0])
			assert.EqualValues(t, docs[1], pruned[1])
			assert.EqualValues(t, docs[2], pruned[2])
			assert.Equal(t, true, hasNext)
		}
		{
			pruned, hasNext := prunePage(1, 3, docs)
			assert.Equal(t, 3, len(pruned))
			assert.EqualValues(t, docs[3], pruned[0])
			assert.EqualValues(t, docs[4], pruned[1])
			assert.EqualValues(t, docs[5], pruned[2])
			assert.Equal(t, true, hasNext)
		}
		{
			pruned, hasNext := prunePage(0, 1, docs)
			assert.Equal(t, 1, len(pruned))
			assert.EqualValues(t, docs[0], pruned[0])
			assert.Equal(t, true, hasNext)
		}
		{
			pruned, hasNext := prunePage(1, 1, docs)
			assert.Equal(t, 1, len(pruned))
			assert.EqualValues(t, docs[1], pruned[0])
			assert.Equal(t, true, hasNext)
		}
		assert.Equal(t, 100, len(docs))

	})
	t.Run("query pipeline", func(t *testing.T) {
		type testCase struct {
			page    int
			limit   int
			ordered bool
		}
		var testCases = []testCase{
			{
				page:    0,
				limit:   10,
				ordered: true,
			},
			{
				page:    0,
				limit:   10,
				ordered: false,
			},
			{
				page:    1,
				limit:   10,
				ordered: true,
			},
			{
				page:    1,
				limit:   10,
				ordered: false,
			},
		}
		for _, tc := range testCases {
			wg := sync.WaitGroup{}
			input := make(chan *Document, 1)
			var results = lo.ToPtr([]*Document{})
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.Nil(t, pipelineQuery(tc.page, tc.limit, OrderBy{}, input, tc.ordered, results))
			}()
			var allDocs []*Document
			for i := 0; i < 100; i++ {
				d := newUserDoc()
				allDocs = append(allDocs, d)
				input <- d
				if tc.ordered && len(*results) >= tc.limit {
					break
				}
			}
			close(input)
			wg.Wait()
			assert.Equal(t, tc.limit, len(*results))
			for i, result := range *results {
				assert.Equal(t, allDocs[i+(tc.page*tc.limit)].GetID(), result.GetID())
			}
		}
	})

}

func newUserDoc() *Document {
	doc, err := NewDocumentFromMap(map[string]interface{}{
		"_id":  gofakeit.UUID(),
		"name": gofakeit.Name(),
		"contact": map[string]interface{}{
			"email": gofakeit.Email(),
		},
		"account_id":      gofakeit.IntRange(0, 100),
		"language":        gofakeit.Language(),
		"birthday_month":  gofakeit.Month(),
		"favorite_number": gofakeit.Second(),
		"gender":          gofakeit.Gender(),
		"age":             gofakeit.IntRange(0, 100),
		"timestamp":       gofakeit.DateRange(time.Now().Truncate(7200*time.Hour), time.Now()),
		"annotations":     gofakeit.Map(),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

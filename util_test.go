package wolverine

import (
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
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

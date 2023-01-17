package myjson

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/autom8ter/myjson/util"
	// import embed package
	_ "embed"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func newUserDoc() *Document {
	doc, err := NewDocumentFrom(map[string]interface{}{
		"_id":  gofakeit.UUID(),
		"name": gofakeit.Name(),
		"contact": map[string]interface{}{
			"email": fmt.Sprintf("%v.%s", gofakeit.IntRange(0, 100), gofakeit.Email()),
		},
		"account_id":      fmt.Sprint(gofakeit.IntRange(0, 100)),
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

var (
	//go:embed testutil/testdata/task.yaml
	taskSchema string
	//go:embed testutil/testdata/user.yaml
	userSchema string
	//go:embed testutil/testdata/account.yaml
	accountSchema  string
	allCollections = [][]byte{[]byte(userSchema), []byte(taskSchema), []byte(accountSchema)}
)

func TestUtil(t *testing.T) {
	type contact struct {
		Email string `json:"email"`
		Phone string `json:"phone,omitempty"`
	}
	type user struct {
		ID      string  `json:"id"`
		Contact contact `json:"contact"`
		Name    string  `json:"name"`
		Age     int     `json:"age"`
		Enabled bool    `json:"enabled"`
	}
	const email = "john.smith@yahoo.com"
	usr := user{
		ID: gofakeit.UUID(),
		Contact: contact{
			Email: email,
			Phone: gofakeit.Phone(),
		},
		Name: "john smith",
		Age:  50,
	}
	r, err := NewDocumentFrom(&usr)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("compareField", func(t *testing.T) {
		d, err := NewDocumentFrom(map[string]any{
			"age":    50,
			"name":   "coleman",
			"isMale": true,
		})
		assert.NoError(t, err)
		d1, err := NewDocumentFrom(map[string]any{
			"age":  51,
			"name": "lacee",
		})
		assert.NoError(t, err)
		t.Run("compare age", func(t *testing.T) {
			assert.False(t, compareField("age", d, d1))
		})
		t.Run("compare age (reverse)", func(t *testing.T) {
			assert.True(t, compareField("age", d1, d))
		})
		t.Run("compare name", func(t *testing.T) {
			assert.False(t, compareField("name", d, d1))
		})
		t.Run("compare name (reverse)", func(t *testing.T) {
			assert.True(t, compareField("name", d1, d))
		})
		t.Run("compare isMale", func(t *testing.T) {
			assert.True(t, compareField("isMale", d, d1))
		})
		t.Run("compare name (reverse)", func(t *testing.T) {
			assert.False(t, compareField("isMale", d1, d))
		})
	})
	t.Run("decode", func(t *testing.T) {
		d, err := NewDocumentFrom(map[string]any{
			"age":    50,
			"name":   "coleman",
			"isMale": true,
		})
		assert.NoError(t, err)
		d1 := NewDocument()
		assert.Nil(t, util.Decode(d, d1))
		assert.Equal(t, d.String(), d1.String())
	})
	t.Run("selectDoc", func(t *testing.T) {
		before := r.Get("contact.email")
		err := selectDocument(r, []Select{{Field: "contact.email"}})
		assert.NoError(t, err)
		after := r.Get("contact.email")
		assert.Equal(t, before, after)
		assert.Nil(t, r.Get("name"))
	})
	t.Run("sum age", func(t *testing.T) {
		var expected = float64(0)
		var docs Documents
		for i := 0; i < 5; i++ {
			u := newUserDoc()
			expected += u.GetFloat("age")
			docs = append(docs, u)
		}
		reduced, err := aggregateDocs(docs, []Select{
			{
				Field:     "age",
				Aggregate: AggregateFunctionSum,
				As:        "age_sum",
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, expected, reduced.GetFloat("age_sum"))
	})
	t.Run("documents - orderBy (desc/desc)", func(t *testing.T) {
		var docs Documents
		for i := 0; i < 100; i++ {
			doc := newUserDoc()
			assert.Nil(t, doc.Set("account_id", gofakeit.IntRange(1, 5)))
			docs = append(docs, doc)
		}
		docs = orderByDocs(docs, []OrderBy{
			{
				Field:     "account_id",
				Direction: OrderByDirectionDesc,
			},
			{
				Field:     "age",
				Direction: OrderByDirectionDesc,
			},
		})
		docs.ForEach(func(next *Document, i int) {
			if len(docs) > i+1 {
				assert.GreaterOrEqual(t, next.GetFloat("account_id"), docs[i+1].GetFloat("account_id"), i)
				if next.GetFloat("account_id") == docs[i+1].GetFloat("account_id") {
					assert.GreaterOrEqual(t, next.GetFloat("age"), docs[i+1].GetFloat("age"), i)
				}
			}
		})
	})
	t.Run("documents - orderBy (asc/asc)", func(t *testing.T) {
		var docs Documents
		for i := 0; i < 100; i++ {
			doc := newUserDoc()
			assert.Nil(t, doc.Set("account_id", gofakeit.IntRange(1, 5)))
			docs = append(docs, doc)
		}
		docs = orderByDocs(docs, []OrderBy{
			{
				Field:     "account_id",
				Direction: OrderByDirectionAsc,
			},
			{
				Field:     "age",
				Direction: OrderByDirectionAsc,
			},
		})
		docs.ForEach(func(next *Document, i int) {
			if len(docs) > i+1 {
				assert.LessOrEqual(t, next.GetFloat("account_id"), docs[i+1].GetFloat("account_id"), i)
				if next.GetFloat("account_id") == docs[i+1].GetFloat("account_id") {
					assert.LessOrEqual(t, next.GetFloat("age"), docs[i+1].GetFloat("age"), i)
				}
			}
		})
	})
	t.Run("documents - orderBy (asc/desc)", func(t *testing.T) {
		var docs Documents
		for i := 0; i < 100; i++ {
			doc := newUserDoc()
			assert.Nil(t, doc.Set("account_id", gofakeit.IntRange(1, 5)))
			docs = append(docs, doc)
		}
		docs = orderByDocs(docs, []OrderBy{
			{
				Field:     "account_id",
				Direction: OrderByDirectionAsc,
			},
			{
				Field:     "age",
				Direction: OrderByDirectionDesc,
			},
		})
		docs.ForEach(func(next *Document, i int) {
			if len(docs) > i+1 {
				assert.LessOrEqual(t, next.GetFloat("account_id"), docs[i+1].GetFloat("account_id"), i)
				if next.GetFloat("account_id") == docs[i+1].GetFloat("account_id") {
					assert.GreaterOrEqual(t, next.GetFloat("age"), docs[i+1].GetFloat("age"), i)
				}
			}
		})
	})
	t.Run("documents - orderBy (desc/asc)", func(t *testing.T) {
		var docs Documents
		for i := 0; i < 100; i++ {
			doc := newUserDoc()
			assert.Nil(t, doc.Set("account_id", gofakeit.IntRange(1, 5)))
			docs = append(docs, doc)
		}
		docs = orderByDocs(docs, []OrderBy{
			{
				Field:     "account_id",
				Direction: OrderByDirectionAsc,
			},
			{
				Field:     "age",
				Direction: OrderByDirectionDesc,
			},
		})
		docs.ForEach(func(next *Document, i int) {
			if len(docs) > i+1 {
				assert.LessOrEqual(t, next.GetFloat("account_id"), docs[i+1].GetFloat("account_id"), i)
				if next.GetFloat("account_id") == docs[i+1].GetFloat("account_id") {
					assert.GreaterOrEqual(t, next.GetFloat("age"), docs[i+1].GetFloat("age"), i)
				}
			}
		})
	})
	t.Run("schemaToCtx", func(t *testing.T) {
		ctx := context.Background()
		s, err := newCollectionSchema([]byte(userSchema))
		assert.NoError(t, err)
		ctx = schemaToCtx(ctx, s)
		s2 := schemaFromCtx(ctx, "user")
		assert.Equal(t, util.JSONString(s), util.JSONString(s2))
	})
	t.Run("defaultAs", func(t *testing.T) {
		assert.Equal(t, "count_account_id", defaultAs(AggregateFunctionCount, "account_id"))
	})

}

package model_test

import (
	"encoding/json"
	"testing"

	"github.com/autom8ter/gokvkit/model"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestDocument(t *testing.T) {
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
	r, err := model.NewDocumentFrom(&usr)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("scan json", func(t *testing.T) {
		var u user
		assert.Nil(t, r.Scan(&u))
		assert.EqualValues(t, u, usr)
	})
	t.Run("get email", func(t *testing.T) {
		assert.Equal(t, usr.Contact.Email, r.Get("contact.email"))
	})
	t.Run("get phone", func(t *testing.T) {
		assert.Equal(t, usr.Contact.Phone, r.GetString("contact.phone"))
	})
	t.Run("get age", func(t *testing.T) {
		assert.Equal(t, float64(usr.Age), r.GetFloat("age"))
	})
	t.Run("get enabled", func(t *testing.T) {
		assert.Equal(t, usr.Enabled, r.GetBool("enabled"))
	})
	t.Run("merge", func(t *testing.T) {
		usr2 := user{ID: usr.ID, Contact: contact{Email: gofakeit.Email()}, Name: "john smith"}
		r2, err := model.NewDocumentFrom(&usr2)
		if err != nil {
			t.Fatal(err)
		}
		err = r.Merge(r2)
		assert.Nil(t, err)
		assert.Equal(t, usr2.Contact.Email, r.GetString("contact.email"))
		assert.Equal(t, usr.Contact.Phone, r.GetString("contact.phone"))
	})
	t.Run("valid", func(t *testing.T) {
		r := model.NewDocument()
		assert.Equal(t, true, r.Valid())
		r, err := model.NewDocumentFrom([]any{1})
		assert.NotNil(t, err)
	})
	t.Run("clone", func(t *testing.T) {
		cloned := r.Clone()
		assert.Equal(t, r.String(), cloned.String())
	})
	t.Run("del", func(t *testing.T) {
		err := r.Del("annotations")
		assert.Nil(t, err)
		val := r.Get("annotations")
		assert.Nil(t, val)
	})
	t.Run("bytes", func(t *testing.T) {
		assert.Equal(t, r.String(), string(r.Bytes()))
	})
	t.Run("new from bytes", func(t *testing.T) {
		n, err := model.NewDocumentFromBytes(r.Bytes())
		assert.Nil(t, err)
		assert.Equal(t, r.String(), string(n.Bytes()))
	})
	t.Run("select", func(t *testing.T) {
		before := r.Get("contact.email")
		err := r.Select([]model.Select{{Field: "contact.email"}})
		assert.Nil(t, err)
		after := r.Get("contact.email")
		assert.Equal(t, before, after)
		assert.Nil(t, r.Get("name"))
	})
	t.Run("set all", func(t *testing.T) {
		c := r.Clone()
		err = c.SetAll(map[string]any{
			"contact.email": gofakeit.Email(),
		})
		assert.Nil(t, err)
		assert.NotEqual(t, r.Get("contact.email"), c.Get("contact.email"))
	})
	t.Run("valid", func(t *testing.T) {

	})

	t.Run("where", func(t *testing.T) {
		r, err = model.NewDocumentFrom(&usr)
		if err != nil {
			t.Fatal(err)
		}
		pass, err := r.Where([]model.Where{
			{
				Field: "contact.email",
				Op:    model.WhereOpEq,
				Value: email,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "contact.email",
				Op:    model.WhereOpContains,
				Value: email,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "contact.email",
				Op:    model.WhereOpEq,
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "contact.email",
				Op:    model.WhereOpNeq,
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpGt,
				Value: 10,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpGte,
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpGte,
				Value: 51,
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpLt,
				Value: 51,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpLte,
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpLte,
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpGte,
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpIn,
				Value: []float64{50},
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    model.WhereOpLt,
				Value: 49,
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]model.Where{
			{
				Field: "age",
				Op:    "8",
				Value: 51,
			},
		})
		assert.NotNil(t, err)
		assert.False(t, pass)
	})
	t.Run("results", func(t *testing.T) {
		var docs = []*model.Document{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		result := model.Page{
			Documents: docs,
			NextPage:  0,
		}
		bits, err := json.Marshal(result)
		assert.Nil(t, err)
		t.Log(string(bits))
	})
	t.Run("documents - for each", func(t *testing.T) {
		var docs = model.Documents{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		count := 0
		docs.ForEach(func(next *model.Document, i int) {
			count++
		})
		assert.Equal(t, 3, count)
	})
	t.Run("documents - filter", func(t *testing.T) {
		var docs = model.Documents{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		docs = docs.Filter(func(document *model.Document, i int) bool {
			return document.String() != docs[0].String()
		})
		assert.Equal(t, 2, len(docs))
	})
	t.Run("documents - slice", func(t *testing.T) {
		var docs = model.Documents{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		docs = docs.Slice(1, 3)
		assert.Equal(t, 2, len(docs))
	})
	t.Run("documents - orderBy", func(t *testing.T) {
		var docs model.Documents
		for i := 0; i < 100; i++ {
			doc := testutil.NewUserDoc()
			assert.Nil(t, doc.Set("account_id", gofakeit.IntRange(1, 5)))
			docs = append(docs, doc)
		}
		docs = model.OrderByDocs(docs, []model.OrderBy{
			{
				Field:     "account_id",
				Direction: model.OrderByDirectionDesc,
			},
			{
				Field:     "age",
				Direction: model.OrderByDirectionDesc,
			},
		})
		docs.ForEach(func(next *model.Document, i int) {
			if len(docs) > i+1 {
				assert.GreaterOrEqual(t, next.GetFloat("account_id"), docs[i+1].GetFloat("account_id"), i)
				if next.GetFloat("account_id") == docs[i+1].GetFloat("account_id") {
					assert.GreaterOrEqual(t, next.GetFloat("age"), docs[i+1].GetFloat("age"), i)
				}
			}
		})
	})
}

func BenchmarkDocument(b *testing.B) {
	b.ReportAllocs()
	doc := testutil.NewUserDoc()

	// BenchmarkDocument/set-12             	  545349	      2145 ns/op	    1744 B/op	       7 allocs/op
	b.Run("set", func(b *testing.B) {
		b.ReportAllocs()
		email := gofakeit.Email()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := doc.Set("contact.email", email)
			assert.Nil(b, err)
		}
	})
	// BenchmarkDocument/get-12         	 3369182	       356.2 ns/op	      16 B/op	       1 allocs/op
	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = doc.Get("contact.email")
		}
	})
}

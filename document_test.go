package myjson_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/testutil"
	"github.com/autom8ter/myjson/util"
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
	r, err := myjson.NewDocumentFrom(&usr)
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
		r2, err := myjson.NewDocumentFrom(&usr2)
		if err != nil {
			t.Fatal(err)
		}
		err = r.Merge(r2)
		assert.NoError(t, err)
		assert.Equal(t, usr2.Contact.Email, r.GetString("contact.email"))
		assert.Equal(t, usr.Contact.Phone, r.GetString("contact.phone"))
	})
	t.Run("valid", func(t *testing.T) {
		r := myjson.NewDocument()
		assert.Equal(t, true, r.Valid())
		r, err := myjson.NewDocumentFrom([]any{1})
		assert.NotNil(t, err)
	})
	t.Run("clone", func(t *testing.T) {
		cloned := r.Clone()
		assert.Equal(t, r.String(), cloned.String())
	})
	t.Run("del", func(t *testing.T) {
		err := r.Del("annotations")
		assert.NoError(t, err)
		val := r.Get("annotations")
		assert.Nil(t, val)
	})
	t.Run("bytes", func(t *testing.T) {
		assert.NotEmpty(t, string(r.Bytes()))
	})
	t.Run("new from bytes", func(t *testing.T) {
		n, err := myjson.NewDocumentFromBytes(r.Bytes())
		assert.NoError(t, err)
		assert.Equal(t, true, n.Valid())
	})
	t.Run("set all", func(t *testing.T) {
		c := r.Clone()
		err = c.SetAll(map[string]any{
			"contact.email": gofakeit.Email(),
		})
		assert.NoError(t, err)
		assert.NotEqual(t, r.Get("contact.email"), c.Get("contact.email"))
	})
	t.Run("diff - none", func(t *testing.T) {
		before := testutil.NewUserDoc()
		diff := before.Diff(before)
		assert.NotNil(t, diff)
		fmt.Println(util.JSONString(&diff))
	})
	t.Run("diff - replace contact.email", func(t *testing.T) {
		before := testutil.NewUserDoc()
		after := before.Clone()
		assert.Nil(t, after.Set("contact.email", gofakeit.Email()))
		diff := after.Diff(before)
		assert.Len(t, diff, 1)
		assert.Equal(t, "contact.email", diff[0].Path)
		assert.Equal(t, myjson.JSONOpReplace, diff[0].Op)
	})
	t.Run("diff - add contact.email", func(t *testing.T) {
		before := testutil.NewUserDoc()
		after := before.Clone()
		assert.Nil(t, before.Del("contact.email"))
		assert.Nil(t, after.Set("contact.email", gofakeit.Email()))
		diff := after.Diff(before)
		assert.Len(t, diff, 1)
		assert.Equal(t, "contact.email", diff[0].Path)
		assert.Equal(t, myjson.JSONOpAdd, diff[0].Op)
		assert.Equal(t, after.Get("contact.email"), diff[0].Value)
	})
	t.Run("diff - remove contact.email", func(t *testing.T) {
		before := testutil.NewUserDoc()
		after := before.Clone()
		assert.Nil(t, after.Del("contact.email"))
		diff := after.Diff(before)
		assert.Len(t, diff, 1)
		assert.Equal(t, "contact.email", diff[0].Path)
		assert.Equal(t, myjson.JSONOpRemove, diff[0].Op)
		assert.Equal(t, before.Get("contact.email"), diff[0].BeforeValue)
	})
	t.Run("apply - remove contact.email", func(t *testing.T) {
		document := testutil.NewUserDoc()
		diff := []myjson.JSONFieldOp{
			{
				Path: "contact.email",
				Op:   myjson.JSONOpRemove,
			},
		}
		assert.NoError(t, document.ApplyOps(diff))
		assert.False(t, document.Exists("contact.email"))
	})
	t.Run("apply - set contact.email", func(t *testing.T) {
		document := testutil.NewUserDoc()
		email := gofakeit.Email()
		diff := []myjson.JSONFieldOp{
			{
				Path:  "contact.email",
				Op:    myjson.JSONOpAdd,
				Value: email,
			},
		}
		assert.NoError(t, document.ApplyOps(diff))
		assert.Equal(t, email, document.Get("contact.email"))
	})
	t.Run("apply - set contact.email then revert", func(t *testing.T) {
		document := testutil.NewUserDoc()
		before := document.Clone()
		assert.NoError(t, document.SetAll(map[string]any{
			"contact.email": gofakeit.Email(),
		}))
		assert.NoError(t, document.Del("age"))
		diff := document.Diff(before)
		assert.NoError(t, document.RevertOps(diff))
		assert.JSONEq(t, before.String(), document.String())
	})
	t.Run("overwrite", func(t *testing.T) {
		before := testutil.NewUserDoc()
		after := before.Clone()
		assert.Nil(t, after.Del("contact.email"))
		assert.NoError(t, before.Overwrite(after.Value()))
		assert.JSONEq(t, after.String(), before.String())
	})

	t.Run("where", func(t *testing.T) {
		r, err = myjson.NewDocumentFrom(&usr)
		if err != nil {
			t.Fatal(err)
		}
		pass, err := r.Where([]myjson.Where{
			{
				Field: "contact.email",
				Op:    myjson.WhereOpEq,
				Value: email,
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "contact.email",
				Op:    myjson.WhereOpContains,
				Value: email,
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "contact.email",
				Op:    myjson.WhereOpEq,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "contact.email",
				Op:    myjson.WhereOpNeq,
				Value: gofakeit.Email(),
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpGt,
				Value: 10,
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpGte,
				Value: 50,
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpGte,
				Value: 51,
			},
		})
		assert.NoError(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpLt,
				Value: 51,
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpLte,
				Value: 50,
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpLte,
				Value: 50,
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpGte,
				Value: 50,
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpIn,
				Value: []float64{50},
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    myjson.WhereOpLt,
				Value: 49,
			},
		})
		assert.NoError(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]myjson.Where{
			{
				Field: "age",
				Op:    "8",
				Value: 51,
			},
		})
		assert.NotNil(t, err)
		assert.False(t, pass)
	})
	t.Run("self ref", func(t *testing.T) {
		usr := testutil.NewUserDoc()
		assert.NoError(t, usr.Set("contact.email", usr.Get("name")))
		pass, err := usr.Where([]myjson.Where{
			{
				Field: "name",
				Op:    myjson.WhereOpEq,
				Value: "$contact.email",
			},
		})
		assert.NoError(t, err)
		assert.True(t, pass)

		pass, err = usr.Where([]myjson.Where{
			{
				Field: "name",
				Op:    myjson.WhereOpNeq,
				Value: "$contact.email",
			},
		})
		assert.NoError(t, err)
		assert.False(t, pass)
	})
	t.Run("mergeJoin", func(t *testing.T) {
		usr := testutil.NewUserDoc()
		tsk := testutil.NewTaskDoc(usr.GetString("_id"))
		assert.Nil(t, usr.MergeJoin(tsk, "tsk"))
		assert.True(t, usr.Exists("tsk"))
		assert.True(t, usr.Exists("tsk.user"))
		assert.True(t, usr.Exists("tsk._id"))
	})
	t.Run("getArray", func(t *testing.T) {
		usr := testutil.NewUserDoc()
		assert.NoError(t, usr.Set("tags", []string{"#colorado"}))
		assert.NotEmpty(t, usr.GetArray("tags"))
		assert.Len(t, usr.GetArray("tags"), 1)
	})
	t.Run("unmarshalJSON", func(t *testing.T) {
		usr := testutil.NewUserDoc()
		bits, err := usr.MarshalJSON()
		assert.NoError(t, err)
		usr2 := myjson.NewDocument()
		assert.NoError(t, usr2.UnmarshalJSON(bits))
		assert.Equal(t, usr.String(), usr2.String())
	})
	t.Run("@reverse", func(t *testing.T) {
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"messages": []string{"hello world", "hello world", "hello"},
		})
		assert.Equal(t, "hello", d.GetArray("messages|@reverse")[0])
	})
	t.Run("@snakeCase", func(t *testing.T) {
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"message": "helloWorld",
		})
		assert.Equal(t, "hello_world", d.Get("message|@snakeCase"))
	})
	t.Run("@camelCase", func(t *testing.T) {
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"message": "hello_world",
		})
		assert.Equal(t, "helloWorld", d.Get("message|@camelCase"))
	})
	t.Run("@kebabCase", func(t *testing.T) {
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"message": "hello_world",
		})
		assert.Equal(t, "hello-world", d.Get("message|@kebabCase"))
	})
	t.Run("@lower", func(t *testing.T) {
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"message": "HELLO WORLD",
		})
		assert.Equal(t, "hello world", d.Get("message|@lower"))
	})
	t.Run("@upper", func(t *testing.T) {
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"message": "hello world",
		})
		assert.Equal(t, "HELLO WORLD", d.Get("message|@upper"))
	})
	t.Run("@replaceAll", func(t *testing.T) {
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"message": "hello world",
		})
		assert.Equal(t, "hello", d.Get(`message|@replaceAll:{"old": " world", "new": ""}`))
	})
	t.Run("@trim", func(t *testing.T) {
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"message": "hello world",
		})
		assert.Equal(t, "helloworld", d.Get(`message|@trim`))
	})
	t.Run("@dateTrunc", func(t *testing.T) {
		date := time.Date(1993, time.August, 17, 0, 0, 0, 0, time.Local)
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"timestamp": date,
		})
		assert.Equal(t, "1993-08-01 00:00:00 +0000 UTC", d.GetString(`timestamp|@dateTrunc:month`))
		assert.Equal(t, "1993-01-01 00:00:00 +0000 UTC", d.GetString(`timestamp|@dateTrunc:year`))
		assert.Equal(t, "1993-08-17 00:00:00 +0000 UTC", d.GetString(`timestamp|@dateTrunc:day`))
	})
	t.Run("@unix", func(t *testing.T) {
		date := time.Date(1993, time.August, 17, 0, 0, 0, 0, time.Local)
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"timestamp": date,
		})
		assert.Equal(t, float64(date.Unix()), d.GetFloat(`timestamp|@unix`))
	})
	t.Run("@unixMilli", func(t *testing.T) {
		date := time.Date(1993, time.August, 17, 0, 0, 0, 0, time.Local)
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"timestamp": date,
		})
		assert.Equal(t, float64(date.UnixMilli()), d.GetFloat(`timestamp|@unixMilli`))
	})
	t.Run("@unixNano", func(t *testing.T) {
		date := time.Date(1993, time.August, 17, 0, 0, 0, 0, time.Local)
		d, _ := myjson.NewDocumentFrom(map[string]any{
			"timestamp": date,
		})
		assert.Equal(t, float64(date.UnixNano()), d.GetFloat(`timestamp|@unixNano`))
	})
	t.Run("results", func(t *testing.T) {
		var docs = []*myjson.Document{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		result := myjson.Page{
			Documents: docs,
			NextPage:  0,
		}
		bits, err := json.Marshal(result)
		assert.NoError(t, err)
		t.Log(string(bits))
	})
	t.Run("documents - for each", func(t *testing.T) {
		var docs = myjson.Documents{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		count := 0
		docs.ForEach(func(next *myjson.Document, i int) {
			count++
		})
		assert.Equal(t, 3, count)
	})
	t.Run("documents - filter", func(t *testing.T) {
		var docs = myjson.Documents{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		docs = docs.Filter(func(document *myjson.Document, i int) bool {
			return document.String() != docs[0].String()
		})
		assert.Equal(t, 2, len(docs))
	})
	t.Run("documents - slice", func(t *testing.T) {
		var docs = myjson.Documents{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		docs = docs.Slice(1, 3)
		assert.Equal(t, 2, len(docs))
	})
	t.Run("documents - map", func(t *testing.T) {
		var docs = myjson.Documents{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		docs.Map(func(t *myjson.Document, i int) *myjson.Document {
			t.Set("age", 1)
			return t
		})
		docs.ForEach(func(next *myjson.Document, i int) {
			assert.Equal(t, float64(1), next.Get("age"))
		})
	})

}

func BenchmarkDocument(b *testing.B) {
	b.ReportAllocs()
	doc := testutil.NewUserDoc()

	// BenchmarkDocument/set-12         	  509811	      2330 ns/op	    1481 B/op	       6 allocs/op
	b.Run("set", func(b *testing.B) {
		b.ReportAllocs()
		email := gofakeit.Email()
		name := gofakeit.Name()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := doc.SetAll(map[string]any{
				"contact.email": email,
				"name":          name,
				"age":           10,
			})
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
	// BenchmarkDocument/where_(2)-12         	  521618	      2254 ns/op	      40 B/op	       3 allocs/op
	b.Run("where (2)", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			doc.Where([]myjson.Where{
				{
					Field: "contact.email",
					Op:    myjson.WhereOpEq,
					Value: doc.Get("contact.email"),
				},
				{
					Field: "age",
					Op:    myjson.WhereOpGte,
					Value: 10,
				},
			})
		}
	})
}

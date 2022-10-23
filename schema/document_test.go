package schema_test

import (
	"encoding/json"
	"github.com/autom8ter/wolverine/schema"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine/internal/testutil"
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
	r, err := schema.NewDocumentFromAny(&usr)
	if err != nil {
		t.Fatal(err)
	}
	r.SetID(usr.ID)
	t.Run("validate", func(t *testing.T) {
		assert.Nil(t, r.Validate())
		c := r.Clone()
		c.Del("_id")
		assert.NotNil(t, c.Validate())
	})
	t.Run("scan json", func(t *testing.T) {
		var u user
		assert.Nil(t, r.ScanJSON(&u))
		assert.EqualValues(t, u, usr)
	})
	t.Run("get id", func(t *testing.T) {
		assert.Equal(t, usr.ID, r.GetID())
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
		r2, err := schema.NewDocumentFromAny(&usr2)
		if err != nil {
			t.Fatal(err)
		}
		r2.SetID(usr.ID)
		r.Merge(r2)
		assert.Equal(t, usr2.Contact.Email, r.GetString("contact.email"))
		assert.Equal(t, usr.Contact.Phone, r.GetString("contact.phone"))
	})
	t.Run("empty", func(t *testing.T) {
		r := schema.NewDocument()
		assert.Equal(t, false, r.Empty())
	})
	t.Run("clone", func(t *testing.T) {
		cloned := r.Clone()
		assert.Equal(t, r.String(), cloned.String())
	})
	t.Run("del", func(t *testing.T) {
		r.Del("annotations")
		val := r.Get("annotations")
		assert.Nil(t, val)
	})
	t.Run("del", func(t *testing.T) {
		r.Del("annotations")
		val := r.Get("annotations")
		assert.Nil(t, val)
	})
	t.Run("del", func(t *testing.T) {
		assert.Equal(t, r.Value()["name"], "john smith")
	})
	t.Run("bytes", func(t *testing.T) {
		assert.Equal(t, r.String(), string(r.Bytes()))
	})
	t.Run("new from bytes", func(t *testing.T) {
		n, err := schema.NewDocumentFromBytes(r.Bytes())
		assert.Nil(t, err)
		assert.Equal(t, r.String(), string(n.Bytes()))
	})
	t.Run("select", func(t *testing.T) {
		before := r.Get("contact.email")
		selected := r.Select([]string{"contact.email"})
		after := selected.Get("contact.email")
		assert.Equal(t, before, after)
		assert.Nil(t, selected.Get("name"))
	})
	t.Run("set all", func(t *testing.T) {
		c := r.Clone()
		c.SetAll(map[string]any{
			"contact.email": gofakeit.Email(),
		})
		assert.NotEqual(t, r.Get("contact.email"), c.Get("contact.email"))
	})

	t.Run("where", func(t *testing.T) {
		r, err = schema.NewDocumentFromAny(&usr)
		if err != nil {
			t.Fatal(err)
		}
		r.SetID(usr.ID)
		pass, err := r.Where([]schema.Where{
			{
				Field: "contact.email",
				Op:    "==",
				Value: email,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "contact.email",
				Op:    "==",
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "contact.email",
				Op:    "!=",
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "age",
				Op:    ">",
				Value: 10,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "age",
				Op:    ">=",
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "age",
				Op:    ">=",
				Value: 51,
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "age",
				Op:    "<",
				Value: 51,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "age",
				Op:    "<=",
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "age",
				Op:    "<=",
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "age",
				Op:    ">=",
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]schema.Where{
			{
				Field: "age",
				Op:    "<",
				Value: 49,
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]schema.Where{
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
		var docs = []*schema.Document{
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
			testutil.NewUserDoc(),
		}
		result := schema.Page{
			Documents: docs,
			NextPage:  0,
		}
		bits, err := json.Marshal(result)
		assert.Nil(t, err)
		t.Log(string(bits))
	})
}

package wolverine_test

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"

	"github.com/autom8ter/wolverine"
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
	r, err := wolverine.NewDocumentFromAny(&usr)
	if err != nil {
		t.Fatal(err)
	}
	r.SetID(usr.ID)
	r.SetCollection("user")

	t.Run("get id", func(t *testing.T) {
		assert.Equal(t, usr.ID, r.GetID())
	})
	t.Run("get email", func(t *testing.T) {
		assert.Equal(t, usr.Contact.Email, r.Get("contact.email"))
	})
	t.Run("get phone", func(t *testing.T) {
		assert.Equal(t, usr.Contact.Phone, r.Get("contact.phone"))
	})
	t.Run("merge", func(t *testing.T) {
		usr2 := user{ID: usr.ID, Contact: contact{Email: gofakeit.Email()}, Name: "john smith"}
		r2, err := wolverine.NewDocumentFromAny(&usr2)
		if err != nil {
			t.Fatal(err)
		}
		r2.SetID(usr.ID)
		r2.SetCollection("user")
		r.Merge(r2)
		assert.Equal(t, usr2.Contact.Email, r.GetString("contact.email"))
		assert.Equal(t, usr.Contact.Phone, r.GetString("contact.phone"))
	})
	t.Run("empty", func(t *testing.T) {
		r := wolverine.NewDocument()
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
	t.Run("select", func(t *testing.T) {
		before := r.Get("contact.email")
		selected := r.Select([]string{"contact.email"})
		after := selected.Get("contact.email")
		assert.Equal(t, before, after)
		assert.Nil(t, selected.Get("name"))
	})
	t.Run("where", func(t *testing.T) {
		r, err = wolverine.NewDocumentFromAny(&usr)
		if err != nil {
			t.Fatal(err)
		}
		r.SetID(usr.ID)
		r.SetCollection("user")
		pass, err := r.Where([]wolverine.Where{
			{
				Field: "contact.email",
				Op:    "==",
				Value: email,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "contact.email",
				Op:    "==",
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "contact.email",
				Op:    "!=",
				Value: gofakeit.Email(),
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    ">",
				Value: 10,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    ">=",
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    ">=",
				Value: 51,
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    "<",
				Value: 51,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    "<=",
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    "<=",
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    ">=",
				Value: 50,
			},
		})
		assert.Nil(t, err)
		assert.True(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    "<",
				Value: 49,
			},
		})
		assert.Nil(t, err)
		assert.False(t, pass)

		pass, err = r.Where([]wolverine.Where{
			{
				Field: "age",
				Op:    "8",
				Value: 51,
			},
		})
		assert.NotNil(t, err)
		assert.False(t, pass)
	})
}

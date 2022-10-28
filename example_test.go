package wolverine_test

import (
	"context"
	"fmt"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/internal/testutil"
	"strings"
)

func getDB() *wolverine.DB {
	config := wolverine.Config{
		Params: map[string]string{
			"provider":     "default",
			"storage_path": "./db",
		},
		Collections: testutil.AllCollections,
	}
	db, err := wolverine.New(context.Background(), config)
	if err != nil {
		panic(err)
	}
	return db
}

func ExampleNew() {
	schema := `
{
  "$id": "https://example.com/user.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "user",
  "type": "object",
  "@collection": "user",
  "@primaryKey": "_id",
  "@indexing": {
    "query": [
      {
        "fields": [
          "contact.email"
        ]
      },
      {
        "fields": [
          "account_id"
        ]
      },
      {
        "fields": [
          "language"
        ]
      }
    ],
    "search": [
      {
        "fields": [
          "contact.email"
        ]
      }
    ]
  },
  "@flags": {},
  "@annotations": {},
  "required": [
    "_id",
    "name",
    "age",
    "contact",
    "gender",
    "account_id"
  ],
  "properties": {
    "_id": {
      "type": "string",
      "description": "The user's id."
    },
    "name": {
      "type": "string",
      "description": "The user's name."
    },
    "contact": {
      "type": "object",
      "properties": {
        "email": {
          "type": "string",
          "description": "The user's email."
        }
      }
    },
    "age": {
      "description": "Age in years which must be equal to or greater than zero.",
      "type": "integer",
      "minimum": 0
    },
    "account_id": {
      "type": "integer",
      "minimum": 0
    },
    "language": {
      "type": "string",
      "description": "The user's first language."
    },
    "gender": {
      "type": "string",
      "description": "The user's gender.",
      "enum": [
        "male",
        "female"
      ]
    },
    "timestamp": {
      "type": "string"
    },
    "annotations": {
      "type": "object"
    }
  }
}`
	userSchema := core.NewCollectionFromBytesP([]byte(schema))
	db, err := wolverine.New(context.Background(), wolverine.Config{
		Params:      map[string]string{},
		Collections: []*core.Collection{userSchema},
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(strings.Join(db.RegisteredCollections(), ","))

	// Output:
	// user
}

func ExampleDB_Close() {
	db := getDB()
	if err := db.Close(context.Background()); err != nil {
		panic(err)
	}
}

func ExampleDB_HasCollection() {
	db := getDB()
	fmt.Println(db.HasCollection("user"))
	// Output:
	// true
}

func ExampleDB_Collection() {
	db := getDB()
	collection := db.Collection("user")
	ctx := context.Background()
	if err := collection.Set(ctx, testutil.NewUserDoc()); err != nil {
		panic(err)
	}
}

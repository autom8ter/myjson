package wolverine_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/autom8ter/wolverine"
	"github.com/autom8ter/wolverine/core"
	"github.com/autom8ter/wolverine/internal/testutil"
	"strings"
)

func getDB() *wolverine.DB {
	config := wolverine.Config{
		StoragePath: "",
		Collections: testutil.AllCollections,
		// add as many custom middlewares as needed
		Middlewares: []core.Middleware{{
			Persist:      []core.PersistWare{},
			Aggregate:    []core.AggregateWare{},
			Search:       []core.SearchWare{},
			Query:        []core.QueryWare{},
			Get:          []core.GetWare{},
			GetAll:       []core.GetAllWare{},
			ChangeStream: []core.ChangeStreamWare{},
		}},
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
		StoragePath: "",
		Collections: []*core.Collection{userSchema},
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(strings.Join(db.RegisteredCollections(), ","))

	// Output:
	// user
}

func ExampleDB_Backup() {
	db := getDB()
	buffer := bytes.NewBuffer(nil)
	err := db.Backup(context.Background(), buffer)
	if err != nil {
		panic(err)
	}
}

func ExampleDB_Restore() {
	db := getDB()
	buffer := bytes.NewBuffer(nil)
	err := db.Restore(context.Background(), buffer)
	if err != nil {
		panic(err)
	}
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
	ctx := context.Background()
	if err := db.Collection(ctx, "user", func(collection *wolverine.Collection) error {
		if err := collection.Set(ctx, testutil.NewUserDoc()); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func ExampleDB_Collections() {
	db := getDB()
	ctx := context.Background()
	if err := db.Collections(ctx, func(collection *wolverine.Collection) error {
		fmt.Println(collection.Schema().Collection())
		return nil
	}); err != nil {
		panic(err)
	}
}

package main

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/kv"
	_ "github.com/autom8ter/myjson/kv/badger"
	"github.com/autom8ter/myjson/testutil"
	"github.com/autom8ter/myjson/transport/openapi"
	"github.com/brianvoe/gofakeit/v6"
)

var (
	//go:embed account.yaml
	accountSchema string
	//go:embed user.yaml
	userSchema string
	//go:embed task.yaml
	taskSchema string
)

func main() {
	ctx := context.Background()
	db, err := myjson.Open(context.Background(), "badger", map[string]any{
		"storage_path": "./tmp",
	})
	if err != nil {
		panic(err)
	}

	if err := db.ConfigureCollection(ctx, []byte(accountSchema)); err != nil {
		panic(err)
	}
	if err := db.ConfigureCollection(ctx, []byte(userSchema)); err != nil {
		panic(err)
	}
	if err := db.ConfigureCollection(ctx, []byte(taskSchema)); err != nil {
		panic(err)
	}

	fmt.Printf("registered collections: %v\n", db.Collections(ctx))

	if err := seed(ctx, db); err != nil {
		panic(err)
	}
	oapi, err := openapi.New(openapi.Config{
		Title:       "Tasks API",
		Version:     "v0.0.0",
		Description: "An example task database api",
		Port:        8080,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("starting openapi http server on port :8080")
	if err := oapi.Serve(ctx, db); err != nil {
		fmt.Println(err)
	}
}

func seed(ctx context.Context, db myjson.Database) error {
	results, err := db.Query(ctx, "account", myjson.Q().Query())
	if err != nil {
		return err
	}
	if results.Count == 0 {
		fmt.Println("seeding database...")
		if err := db.Tx(ctx, kv.TxOpts{}, func(ctx context.Context, tx myjson.Tx) error {
			for i := 0; i < 100; i++ {
				a, _ := myjson.NewDocumentFrom(map[string]any{
					"name": gofakeit.Company(),
				})
				accountID, err := tx.Create(ctx, "account", a)
				if err != nil {
					return err
				}
				for i := 0; i < 10; i++ {
					u := testutil.NewUserDoc()
					u.Set("account_id", accountID)
					usrID, err := tx.Create(ctx, "user", u)
					if err != nil {
						return err
					}
					for i := 0; i < 3; i++ {
						t := testutil.NewTaskDoc(usrID)
						_, err := tx.Create(ctx, "task", t)
						if err != nil {
							return err
						}
					}
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("failed to seed database: %s", err.Error())
		}
		fmt.Println("successfully seeded database")
	}
	return nil
}

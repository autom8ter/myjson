package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/brianvoe/gofakeit/v6"

	"wolverine"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := wolverine.New(ctx, wolverine.Config{
		Path:  "task.db",
		Debug: true,

		Collections: []wolverine.Collection{
			{
				Name: "task",

				Indexes: []wolverine.Index{
					{
						Fields: []string{"owner"},
					},
					{
						Fields: []string{"account_id", "done"},
					},
					{
						Fields:   []string{"*"},
						FullText: true,
					},
				},
			},
		},
		Migrations: []wolverine.Migration{
			{
				Name: "seed_task",
				Function: func(ctx context.Context, db wolverine.DB) error {
					var tasks []wolverine.Record
					for i := 0; i < 1000; i++ {
						tasks = append(tasks, map[string]interface{}{
							"_id":         gofakeit.UUID(),
							"_collection": "task",
							"account_id":  gofakeit.IntRange(0, 50),
							"owner":       gofakeit.Email(),
							"content":     gofakeit.LoremIpsumSentence(15),
							"done":        gofakeit.Bool(),
							"created_at":  gofakeit.Date().Unix(),
						})
					}
					if err := db.BatchSet(ctx, tasks); err != nil {
						return err
					}
					return nil
				},
			},
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close(ctx)

	handler, err := wolverine.Handler(db)
	if err != nil {
		db.Error(ctx, "failed to setup handler", err, map[string]interface{}{})
		return
	}
	addr := ":8080"
	db.Info(ctx, "starting server", map[string]interface{}{"addr": addr})
	if err := http.ListenAndServe(addr, handler); err != nil {
		db.Error(ctx, "server failure", err, map[string]interface{}{"addr": addr})
	}
}

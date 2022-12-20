package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/model"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func Test(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		assert.Nil(t, testutil.TestDB(func(ctx context.Context, db *gokvkit.DB) {
			egp, ctx := errgroup.WithContext(ctx)
			for i := 0; i < 100; i++ {
				err := db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
					return tx.Set(ctx, "user", testutil.NewUserDoc())
				})
				assert.Nil(t, err)
				egp.Go(func() error {
					err := db.Tx(ctx, true, func(ctx context.Context, tx gokvkit.Tx) error {
						results, err := tx.Query(ctx, "user", gokvkit.NewQueryBuilder().
							Select(model.Select{Field: "*"}).
							Where(model.Where{
								Field: "contact.email",
								Op:    model.WhereOpEq,
								Value: gofakeit.Email(),
							}).Query())
						if err != nil {
							return err
						}
						fmt.Println(results.Stats)
						return nil
					})
					if err != nil {
						return err
					}
					return nil
				})
				time.Sleep(200 * time.Millisecond)
			}
			for i := 0; i < 5; i++ {
				egp.Go(func() error {
					{
						//schema := db.GetSchema("user")
						//assert.Nil(t, schema.SetIndex(model.Index{
						//	Name:    "email_idx",
						//	Fields:  []string{"contact.email"},
						//	Unique:  true,
						//	Primary: false,
						//}))
						//bytes, err := schema.Bytes()
						//assert.Nil(t, err)
						//assert.Nil(t, db.ConfigureCollection(ctx, bytes))
					}
					{
						schema := db.GetSchema("user")
						assert.Nil(t, schema.DelIndex("email_idx"))
						bytes, err := schema.Bytes()
						assert.Nil(t, err)
						assert.Nil(t, db.ConfigureCollection(ctx, bytes))
					}

					return nil
				})
			}
			assert.Nil(t, egp.Wait())
		}))
	})
}

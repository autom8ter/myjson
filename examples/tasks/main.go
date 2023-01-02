package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/kv"
	_ "github.com/autom8ter/gokvkit/kv/badger"
	"github.com/autom8ter/gokvkit/testutil"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/go-chi/chi/v5"
	"github.com/samber/lo"
	"gopkg.in/yaml.v2"
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
	db, err := gokvkit.New(context.Background(), "badger", map[string]any{
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

	mux := chi.NewRouter()

	mux.Get("/collections", func(w http.ResponseWriter, r *http.Request) {
		collections := db.Collections(r.Context())
		var schemas = map[string]string{}
		for _, c := range collections {
			bits, _ := db.GetSchema(ctx, c).MarshalYAML()
			schemas[c] = string(bits)
		}
		yaml.NewEncoder(w).Encode(&schemas)
	})
	mux.Post("/collections/{collection}/documents", func(w http.ResponseWriter, r *http.Request) {
		c := chi.URLParam(r, "collection")
		if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
			bits, err := ioutil.ReadAll(r.Body)
			if err != nil {

			}
			doc, err := gokvkit.NewDocumentFromBytes(bits)
			if err != nil {
				return err
			}
			_, err = tx.Create(r.Context(), c, doc)
			if err != nil {
				return err
			}
			w.Write(doc.Bytes())
			return nil
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.Get("/collections/{collection}/documents/{id}", func(w http.ResponseWriter, r *http.Request) {
		c := chi.URLParam(r, "collection")
		id := chi.URLParam(r, "id")
		if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: true}, func(ctx context.Context, tx gokvkit.Tx) error {
			doc, err := tx.Get(r.Context(), c, id)
			if err != nil {
				return err
			}
			w.Write(doc.Bytes())
			return nil
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.Put("/collections/{collection}/documents/{id}", func(w http.ResponseWriter, r *http.Request) {
		c := chi.URLParam(r, "collection")
		//id := chi.URLParam(r, "id")
		if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
			bits, err := ioutil.ReadAll(r.Body)
			if err != nil {

			}
			doc, err := gokvkit.NewDocumentFromBytes(bits)
			if err != nil {
				return err
			}
			if err := tx.Set(r.Context(), c, doc); err != nil {
				return err
			}
			w.Write(doc.Bytes())
			return nil
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.Patch("/collections/{collection}/documents/{id}", func(w http.ResponseWriter, r *http.Request) {
		c := chi.URLParam(r, "collection")
		id := chi.URLParam(r, "id")
		if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
			bits, err := ioutil.ReadAll(r.Body)
			if err != nil {

			}
			doc, err := gokvkit.NewDocumentFromBytes(bits)
			if err != nil {
				return err
			}
			if err := tx.Update(r.Context(), c, id, doc.Value()); err != nil {
				return err
			}
			w.Write(doc.Bytes())
			return nil
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.Delete("/collections/{collection}/documents/{id}", func(w http.ResponseWriter, r *http.Request) {
		c := chi.URLParam(r, "collection")
		id := chi.URLParam(r, "id")
		if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
			if err := tx.Delete(r.Context(), c, id); err != nil {
				return err
			}
			w.WriteHeader(http.StatusOK)
			return nil
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.Post("/collections/{collection}/query", func(w http.ResponseWriter, r *http.Request) {
		c := chi.URLParam(r, "collection")
		if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: true}, func(ctx context.Context, tx gokvkit.Tx) error {
			var query gokvkit.Query
			json.NewDecoder(r.Body).Decode(&query)
			results, err := tx.Query(r.Context(), c, query)
			if err != nil {
				return err
			}
			bits, err := json.Marshal(results)
			if err != nil {
				return err
			}
			w.Write(bits)
			return nil
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	listRoutes(mux.Routes())

	fmt.Println("starting http server on port :8080")
	http.ListenAndServe(":8080", mux)
}

func listRoutes(routes []chi.Route) {
	for _, r := range routes {
		if r.SubRoutes != nil && len(r.SubRoutes.Routes()) > 0 {
			listRoutes(routes)
		}
		fmt.Printf("registered endpoint: %v %v\n", lo.Keys(r.Handlers), r.Pattern)
	}
}

func seed(ctx context.Context, db gokvkit.Database) error {
	results, err := db.Query(ctx, "account", gokvkit.Q().Query())
	if err != nil {
		return err
	}
	if results.Count == 0 {
		fmt.Println("seeding database...")
		if err := db.Tx(ctx, kv.TxOpts{}, func(ctx context.Context, tx gokvkit.Tx) error {
			for i := 0; i < 100; i++ {
				a, _ := gokvkit.NewDocumentFrom(map[string]any{
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

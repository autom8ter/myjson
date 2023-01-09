package openapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/extentions/openapi/httpError"
	"github.com/autom8ter/myjson/kv"
	"github.com/gorilla/mux"
)

func (o *OpenAPIServer) batchSetHandler(db myjson.Database) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		collection := mux.Vars(r)["collection"]
		if !db.HasCollection(r.Context(), collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		var docs []*myjson.Document
		if err := json.NewDecoder(r.Body).Decode(&docs); err != nil {
			httpError.Error(w, errors.Wrap(err, errors.Validation, "failed to decode query"))
			return
		}
		if err := db.Tx(r.Context(), kv.TxOpts{IsBatch: true}, func(ctx context.Context, tx myjson.Tx) error {
			for _, d := range docs {
				if err := tx.Set(ctx, collection, d); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

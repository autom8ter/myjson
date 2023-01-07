package openapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/kv"
	"github.com/autom8ter/myjson/transport/openapi/httpError"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/mux"
)

func (o *openAPIServer) patchDocHandler(db myjson.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := mux.Vars(r)["collection"]
		if !db.HasCollection(r.Context(), collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		var update = map[string]any{}
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to decode edit"))
			return
		}
		if err := db.Tx(r.Context(), kv.TxOpts{}, func(ctx context.Context, tx myjson.Tx) error {
			err := tx.Update(ctx, collection, docID, update)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		doc, err := db.Get(r.Context(), collection, docID)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to edit document"))
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}

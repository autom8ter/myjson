package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"
	"github.com/autom8ter/gokvkit/model"
	"github.com/go-chi/chi/v5"
)

func CreateDocHandler(o api.OpenAPIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, errors.New(errors.NotFound, "collection does not exist"))
			return
		}
		var doc = model.NewDocument()
		if err := json.NewDecoder(r.Body).Decode(doc); err != nil {
			httpError.Error(w, errors.Wrap(err, errors.Validation, "failed to decode query"))
			return
		}
		if err := o.DB().Tx(r.Context(), true, func(ctx context.Context, tx gokvkit.Tx) error {
			id, err := tx.Create(ctx, collection, doc)
			if err != nil {
				return err
			}
			if err := o.DB().GetSchema(collection).SetPrimaryKey(doc, id); err != nil {
				return err
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}

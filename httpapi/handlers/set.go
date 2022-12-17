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

func SetDocHandler(o api.OpenAPIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		var doc = model.NewDocument()
		if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to decode update"))
			return
		}
		if err := o.DB().GetSchema(collection).SetPrimaryKey(doc, docID); err != nil {
			httpError.Error(w, errors.New(errors.Validation, "bad id: %s", docID))
			return
		}
		if err := o.DB().Tx(r.Context(), true, func(ctx context.Context, tx gokvkit.Tx) error {
			err := tx.Set(ctx, collection, doc)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			httpError.Error(w, err)
			return
		}
		doc, err := o.DB().Get(r.Context(), collection, docID)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to edit document"))
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}

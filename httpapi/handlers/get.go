package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"
	"github.com/go-chi/chi/v5"
)

func GetDocHandler(o api.OpenAPIServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		doc, err := o.DB().Get(r.Context(), collection, docID)
		if err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusNotFound, "failed to get document"))
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}

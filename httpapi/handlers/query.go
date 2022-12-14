package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/autom8ter/gokvkit/errors"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"
	"github.com/autom8ter/gokvkit/model"
	"github.com/go-chi/chi/v5"
)

func QueryHandler(o api.OpenAPIServer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, errors.New(errors.Validation, "collection does not exist"))
			return
		}
		var q model.Query
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "failed to decode query"))
			return
		}
		results, err := o.DB().Query(r.Context(), collection, q)
		if err != nil {
			httpError.Error(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&results)
	})
}

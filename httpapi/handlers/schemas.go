package handlers

import (
	"io"
	"net/http"

	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"
	"github.com/go-chi/chi/v5"
	"github.com/palantir/stacktrace"
	"gopkg.in/yaml.v2"
)

func GetSchemasHandler(o api.OpenAPIServer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var resp = map[string]any{}
		var collections = o.DB().Collections()
		for _, c := range collections {
			schema, ok := o.DB().CollectionSchema(c)
			if ok {
				resp[c] = string(schema)
			}
		}
		w.WriteHeader(http.StatusOK)
		yaml.NewEncoder(w).Encode(&resp)
	})
}

func GetSchemaHandler(o api.OpenAPIServer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !o.DB().HasCollection(collection) {
			httpError.Error(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		schema, _ := o.DB().CollectionSchema(collection)
		w.WriteHeader(http.StatusOK)
		w.Write(schema)
	})
}

func PutSchemaHandler(o api.OpenAPIServer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bits, err := io.ReadAll(r.Body)
		if err != nil {
			httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to read request body"))
			return
		}
		if err := o.DB().ConfigureCollection(r.Context(), bits); err != nil {
			httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to configure collection"))
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

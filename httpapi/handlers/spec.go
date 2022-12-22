package handlers

import (
	"net/http"

	"github.com/autom8ter/gokvkit/httpapi/api"
)

func SpecHandler(o api.OpenAPIServer) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bits, _ := o.Spec(r.Context())
		w.WriteHeader(http.StatusOK)
		w.Write(bits)
	})
}

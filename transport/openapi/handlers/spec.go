package handlers

import (
	"context"
	"net/http"
)

func SpecHandler(specFunc func(ctx context.Context) ([]byte, error)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bits, _ := specFunc(r.Context())
		w.WriteHeader(http.StatusOK)
		w.Write(bits)
	})
}

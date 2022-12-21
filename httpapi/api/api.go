package api

import (
	"context"
	"net/http"

	"github.com/autom8ter/gokvkit"
)

type OpenAPIServer interface {
	DB() gokvkit.Database
	Spec() ([]byte, error)
	Handler() http.Handler
	Serve(ctx context.Context, port int) error
}

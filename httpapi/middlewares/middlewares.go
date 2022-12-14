package middlewares

import (
	"context"
	"fmt"
	"net/http"

	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/httpError"
	"github.com/autom8ter/gokvkit/model"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/palantir/stacktrace"
)

func MetadataInjector() func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			md, _ := model.GetMetadata(r.Context())
			for k, v := range r.Header {
				md.Set(fmt.Sprintf("http.header.%s", k), v)
			}
			handler.ServeHTTP(w, r.WithContext(md.ToContext(r.Context())))
		})
	}
}

func OpenAPIValidator(o api.OpenAPIServer) func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bits, _ := o.Spec()
			loader := openapi3.NewLoader()
			doc, _ := loader.LoadFromData(bits)
			err := doc.Validate(loader.Context)
			if err != nil {
				httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusInternalServerError, "invalid openapi spec"))
				return
			}
			router, err := gorillamux.NewRouter(doc)
			if err != nil {
				httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to configure collection"))
				return
			}
			md, _ := model.GetMetadata(r.Context())
			route, pathParams, err := router.FindRoute(r)
			if err != nil {
				httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusNotFound, "route not found"))
				return
			}
			requestValidationInput := &openapi3filter.RequestValidationInput{
				Request:    r,
				PathParams: pathParams,
				Route:      route,
				Options: &openapi3filter.Options{AuthenticationFunc: func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
					return nil
				}},
			}
			if err := openapi3filter.ValidateRequest(r.Context(), requestValidationInput); err != nil {
				httpError.Error(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, ""))
				return
			}
			md.SetAll(map[string]any{
				"openapi.path_params": pathParams,
				"openapi.route":       route.Path,
			})
			handler.ServeHTTP(w, r.WithContext(md.ToContext(r.Context())))
		})
	}
}

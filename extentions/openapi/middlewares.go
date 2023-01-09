package openapi

import (
	"context"
	"fmt"
	"net/http"

	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/errors"
	"github.com/autom8ter/myjson/extentions/openapi/httpError"
	"github.com/gorilla/mux"

	"github.com/getkin/kin-openapi/openapi3filter"
)

// OpenAPIValidator validates inbound requests against the openapi schema
// adds openapi.path_params to the inbound metadata
// adds openapi.route to the inbound metadata
// adds openapi.header.${headerName} to the metadata
func (o *OpenAPIServer) openAPIValidator() mux.MiddlewareFunc {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			o.specMu.RLock()
			defer o.specMu.RUnlock()
			md, _ := myjson.GetMetadata(r.Context())
			for k, v := range r.Header {
				md.Set(fmt.Sprintf("openapi.header.%s", k), v)
			}
			route, pathParams, err := o.openapiRouter.FindRoute(r)
			if err != nil {
				httpError.Error(w, errors.Wrap(err, http.StatusNotFound, "route not found"))
				return
			}
			requestValidationInput := &openapi3filter.RequestValidationInput{
				Request:    r,
				PathParams: pathParams,
				Route:      route,
				Options: &openapi3filter.Options{AuthenticationFunc: func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
					//TODO: add auth
					return nil
				}},
			}
			if err := openapi3filter.ValidateRequest(r.Context(), requestValidationInput); err != nil {
				httpError.Error(w, errors.Wrap(err, http.StatusBadRequest, "request failed validation"))
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

package openapi

type Opt func(*OpenAPIServer)

func WithLogger(logger Logger) Opt {
	return func(o *OpenAPIServer) {
		o.logger = logger
	}
}

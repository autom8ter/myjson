package wolverine

// Middleware wraps a CoreAPI instance and returns a new one
type Middleware interface {
	Apply(c CoreAPI) CoreAPI
}

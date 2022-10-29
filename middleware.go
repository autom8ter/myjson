package wolverine

type Middleware interface {
	Apply(c CoreAPI) CoreAPI
}

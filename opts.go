package myjson

// DBOpt is an option for configuring a collection
type DBOpt func(d *defaultDB)

// WithOptimizer overrides the default query optimizer provider
func WithOptimizer(o Optimizer) DBOpt {
	return func(d *defaultDB) {
		d.optimizer = o
	}
}

// WithJavascriptOverrides adds global variables or methods to the embedded javascript vm(s)
func WithJavascriptOverrides(overrides map[string]any) DBOpt {
	return func(d *defaultDB) {
		d.jsOverrides = overrides
	}
}

// WithPersistCDC configures the database to persist all change-data-capture entries so that features like time-travel and
// change streaming are possible
func WithPersistCDC(persist bool) DBOpt {
	return func(d *defaultDB) {
		d.persistCDC = persist
	}
}

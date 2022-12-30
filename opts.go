package gokvkit

// DBOpt is an option for configuring a collection
type DBOpt func(d *defaultDB)

// WithOptimizer overrides the default query optimizer provider
func WithOptimizer(o Optimizer) DBOpt {
	return func(d *defaultDB) {
		d.optimizer = o
	}
}

// WithChangeStream overrides the default change stream provider so a distributed provider can be configured ex: nats, rabbitmq, kafka
func WithChangeStream(c Stream[CDC]) DBOpt {
	return func(d *defaultDB) {
		d.cdcStream = c
	}
}

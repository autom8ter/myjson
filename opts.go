package gokvkit

// DBOpt is an option for configuring a collection
type DBOpt func(d *defaultDB)

// WithOnPersist adds a hook to the collections configuration that executes on changes as commands are persisted
func WithOnPersist(onPersist []OnPersist) DBOpt {
	return func(d *defaultDB) {
		d.persistHooks = append(d.persistHooks, onPersist...)
	}
}

// WithOnInit adds database initializers(s) to the database.
func WithOnInit(onInit []OnInit) DBOpt {
	return func(d *defaultDB) {
		d.initHooks = append(d.initHooks, onInit...)
	}
}

// WithOnCommit adds database hooks that execute before a transaction is commited
func WithOnCommit(onCommit ...OnCommit) DBOpt {
	return func(d *defaultDB) {
		d.onCommit = append(d.onCommit, onCommit...)
	}
}

// WithOnRollback adds database hooks that execute before a transaction is rolled back
func WithOnRollback(onRollback ...OnRollback) DBOpt {
	return func(d *defaultDB) {
		d.onRollback = append(d.onRollback, onRollback...)
	}
}

// WithOptimizer overrides the default query optimizer provider
func WithOptimizer(o Optimizer) DBOpt {
	return func(d *defaultDB) {
		d.optimizer = o
	}
}

// WithChangeStream overrides the default change stream provider
func WithChangeStream(c Stream[CDC]) DBOpt {
	return func(d *defaultDB) {
		d.cdcStream = c
	}
}

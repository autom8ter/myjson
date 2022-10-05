package wolverine

// Config configures a database instance
type Config struct {
	// Path is the path to database storage. Use 'inmem' to operate the database in memory only.
	Path string
	// Debug sets the database to debug level
	Debug bool
	// Migrate, if a true, has the database run any migrations that have not already run(idempotent).
	Migrate bool
	// ReIndex reindexes the database
	ReIndex bool
	// CronJob are cron jobs that will run
	CronJobs []CronJob
	// BeforeSet is a hook that is executed before a set operation
	BeforeSet WriteTrigger
	// BeforeSet is a hook that is executed before a set operation
	BeforeUpdate WriteTrigger
	// BeforeDelete is a hook that is executed before a delete operation
	BeforeDelete WriteTrigger
	// OnRead is a hook that is executed as a record is read
	OnRead ReadTrigger
	// OnStream is a hook that is executed as a record is sent to a stream
	OnStream ReadTrigger
	// AfterSet is a hook that is executed after a set operation
	AfterSet WriteTrigger
	// AfterUpdate is a hook that is executed after an update operation
	AfterUpdate WriteTrigger
	// AfterDelete is a hook that is executed after a delete operation
	AfterDelete WriteTrigger
	// Migrations are atomic database migrations to run on startup
	Migrations []Migration
}

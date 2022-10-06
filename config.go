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
	// Migrations are atomic database migrations to run on startup
	Migrations []Migration
}

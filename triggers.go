package wolverine

import "context"

// ReadTrigger is a function that is executed in response to an readable action taken against the database
// Triggers should be use to create side effects based on the context and the data associated with a database action
type ReadTrigger func(db DB, ctx context.Context, document *Document) error

// WriteTrigger is a function that is executed in response to a writeable action taken against the database
// Hooks should be use to create side effects based on the context and the data associated with a database action
type WriteTrigger func(db DB, ctx context.Context, before, after *Document) error

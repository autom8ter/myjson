package wolverine

import (
	"context"
)

// CronJob is a function that runs against the database on a given crontab schedule
type CronJob struct {
	// Schedule is the cron schedule
	Schedule string
	// Function is the function executed by the cron
	Function func(ctx context.Context, db DB)
}

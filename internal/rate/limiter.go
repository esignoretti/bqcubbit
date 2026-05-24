package rate

import (
	"context"

	"golang.org/x/time/rate"
)

type Limiters struct {
	BQReadSessions *rate.Limiter
	BQExportJobs   *rate.Limiter
	CubbitUploads  *rate.Limiter
}

func NewLimiters(bqReadPerHour, bqExportPerHour, cubbitPerMin int) *Limiters {
	return &Limiters{
		BQReadSessions: rate.NewLimiter(rate.Limit(bqReadPerHour)/3600, 1),
		BQExportJobs:   rate.NewLimiter(rate.Limit(bqExportPerHour)/3600, 1),
		CubbitUploads:  rate.NewLimiter(rate.Limit(cubbitPerMin)/60, 1),
	}
}

func (l *Limiters) WaitBQRead(ctx context.Context) error {
	return l.BQReadSessions.Wait(ctx)
}

func (l *Limiters) WaitBQExport(ctx context.Context) error {
	return l.BQExportJobs.Wait(ctx)
}

func (l *Limiters) WaitUpload(ctx context.Context) error {
	return l.CubbitUploads.Wait(ctx)
}

func (l *Limiters) WaitAll(ctx context.Context) error {
	return nil
}

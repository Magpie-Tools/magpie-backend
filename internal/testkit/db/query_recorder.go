package testdb

import (
	"context"
	"time"

	"gorm.io/gorm/logger"
)

// QueryRecorder is a GORM logger that records the SQL of every statement and
// passes each one on to the logger it wraps. Install it on a session with
// db.Session(&gorm.Session{Logger: recorder}). It is not safe for concurrent
// statements.
type QueryRecorder struct {
	logger.Interface
	Queries []string
}

// Trace records the statement and forwards it.
func (l *QueryRecorder) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, _ := fc()
	l.Queries = append(l.Queries, sql)
	l.Interface.Trace(ctx, begin, fc, err)
}

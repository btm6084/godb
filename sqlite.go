package godb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/btm6084/utilities/metrics"
	"github.com/btm6084/utilities/stack"
	log "github.com/sirupsen/logrus"

	// Causes side effects in database/sql and allows us to connect to sqlserver.
	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDatastore is an implementation of SQLiteSQL datastore for golang.
type SQLiteDatastore struct {
	db *sql.DB
}

// NewSQLiteDatastore configures and returns a usable SQLiteDatastore
func NewSQLiteDatastore(file string, maxOpen, maxIdle int) *SQLiteDatastore {
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		log.WithFields(stack.TraceFields()).Println(err)
		return nil
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	store := &SQLiteDatastore{db}

	err = store.Ping(context.Background())
	if err != nil {
		log.WithFields(stack.TraceFields()).Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (s *SQLiteDatastore) Ping(ctx context.Context) error {
	if s == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	// This will choose the default recorder chosen during setup. If metrics.MetricsRecorder is never changed,
	// this will default to the noop recorder.
	r := metrics.GetRecorder(ctx)

	var result []string
	end := r.DatabaseSegment("mssql", "SELECT strftime('%s', 'now');")
	rows, err := s.db.QueryContext(ctx, "SELECT strftime('%s', 'now');")
	end()
	if err != nil {
		return err
	}

	defer rows.Close()
	Unmarshal(rows, &result)

	if len(result) < 1 {
		return errors.New("Ping Failed")
	}

	return err
}

// Shutdown performs any closing operations. Best called as deferred from main after the datastore is initialized.
func (s *SQLiteDatastore) Shutdown(context.Context) error {
	if s != nil && s.db == nil {
		return fmt.Errorf("no valid database")
	}

	s.db.Close()
	return nil
}

// Stats returns statistics about the current DB connection.
func (s *SQLiteDatastore) Stats(context.Context) sql.DBStats {
	if s != nil && s.db != nil {
		s.db.Stats()
	}
	return sql.DBStats{}
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (s *SQLiteDatastore) Fetch(ctx context.Context, query string, container interface{}, args ...interface{}) error {
	if s == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	defer rows.Close()
	err = Unmarshal(rows, &container)
	return err
}

// FetchWithMetrics provides a simple query-and-get operation. We will run your query and fill your container.
func (s *SQLiteDatastore) FetchWithMetrics(ctx context.Context, r metrics.Recorder, query string, container interface{}, args ...interface{}) error {
	if s == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("sqlite3", query, args...)
	rows, err := s.db.QueryContext(ctx, query, args...)
	end()
	if err != nil {
		return err
	}

	defer rows.Close()

	end = r.Segment("GODB::FetchWithMetrics::UnmarshalWithMetrics")
	err = UnmarshalWithMetrics(r, rows, &container)
	end()
	return err
}

// FetchJSON provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (s *SQLiteDatastore) FetchJSON(ctx context.Context, query string, args ...interface{}) ([]byte, error) {
	if s == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return ToJSON(rows)
}

// FetchJSONWithMetrics provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (s *SQLiteDatastore) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) ([]byte, error) {
	if s == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("sqlite3", query, args...)
	rows, err := s.db.QueryContext(ctx, query, args...)
	end()
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	end = r.Segment("GODB::FetchWithMetrics::FetchJSONWithMetrics")
	j, err := ToJSON(rows)
	end()

	return j, err
}

// Exec provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (s *SQLiteDatastore) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if s == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	return s.db.ExecContext(ctx, query, args...)
}

// ExecWithMetrics provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (s *SQLiteDatastore) ExecWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) (sql.Result, error) {
	if s == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("sqlite3", query, args...)
	res, err := s.db.ExecContext(ctx, query, args...)
	end()

	return res, err
}

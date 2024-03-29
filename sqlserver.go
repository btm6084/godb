package godb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/btm6084/utilities/metrics"
	"github.com/btm6084/utilities/stack"

	// Causes side effects in database/sql and allows us to connect to sqlserver.
	_ "github.com/denisenkom/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

// MSSQLDatastore is an implementation of MSSQLDatastore datastore for golang.
type MSSQLDatastore struct {
	db *sql.DB
}

// NewMSSQLDatastore configures and returns a usable MSSQLDatastore from parameters.
func NewMSSQLDatastore(user, pass, dbName, host, port, appname string, maxOpen, maxIdle int) *MSSQLDatastore {
	query := url.Values{}
	query.Add("database", dbName)
	query.Add("app name", appname)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, pass),
		Host:     fmt.Sprintf("%s:%s", host, port),
		RawQuery: query.Encode(),
	}

	return NewMSSQLDatastoreCS(u.String(), maxOpen, maxIdle)
}

// NewMSSQLDatastoreCS configures and returns a usable MSSQLDatastore from a connect string.
func NewMSSQLDatastoreCS(connectString string, maxOpen, maxIdle int) *MSSQLDatastore {
	connectString = strings.ReplaceAll(connectString, "\r", "")
	connectString = strings.ReplaceAll(connectString, "\n", "")
	db, err := sql.Open("sqlserver", connectString)
	if err != nil {
		log.WithFields(stack.TraceFields()).Println(err)
		return nil
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	store := &MSSQLDatastore{db}

	err = store.Ping(context.Background())
	if err != nil {
		log.WithFields(stack.TraceFields()).Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (m *MSSQLDatastore) Ping(ctx context.Context) error {
	if m == nil {
		return ErrEmptyObject
	}

	// This will choose the default recorder chosen during setup. If metrics.MetricsRecorder is never changed,
	// this will default to the noop recorder.
	r := metrics.GetRecorder(ctx)

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	var result []string
	end := r.DatabaseSegment("mssql", "select getdate()")
	rows, err := m.db.QueryContext(ctx, "select getdate()")
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
func (m *MSSQLDatastore) Shutdown(context.Context) error {
	if m != nil && m.db != nil {
		m.db.Close()
	}
	return nil
}

// Stats returns statistics about the current DB connection.
func (m *MSSQLDatastore) Stats(context.Context) sql.DBStats {
	if m != nil && m.db != nil {
		return m.db.Stats()
	}
	return sql.DBStats{}
}

// Begin starts a single transaction. You MUST call Transaction.Rollback, or Transaction.Commit after calling Begin, or you WILL
// leak memory.
// It is safe to defer Transaction.Rollback immediately, even if you don't intend to rollback.
// Once you Commit, Rollback becomes a no-op.
func (m *MSSQLDatastore) BeginTx(ctx context.Context) (Transaction, error) {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	return &MSSQLTx{db: m.db, tx: tx}, nil
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (m *MSSQLDatastore) Fetch(ctx context.Context, query string, container interface{}, args ...interface{}) error {
	return m.FetchWithMetrics(ctx, &metrics.NoOp{}, query, container, args...)
}

// FetchWithMetrics provides a simple query-and-get operation. We will run your query and fill your container.
func (m *MSSQLDatastore) FetchWithMetrics(ctx context.Context, r metrics.Recorder, query string, container interface{}, args ...interface{}) error {
	if m == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("mssql", query, args...)
	rows, err := m.db.QueryContext(ctx, query, args...)
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
func (m *MSSQLDatastore) FetchJSON(ctx context.Context, query string, args ...interface{}) ([]byte, error) {
	if m == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return ToJSON(rows)
}

// FetchJSONWithMetrics provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (m *MSSQLDatastore) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) ([]byte, error) {
	if m == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("mssql", query, args...)
	rows, err := m.db.QueryContext(ctx, query, args...)
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
func (m *MSSQLDatastore) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if m == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	return m.db.ExecContext(ctx, query, args...)
}

// ExecWithMetrics provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (m *MSSQLDatastore) ExecWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) (sql.Result, error) {
	if m == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("mssql", query, args...)
	res, err := m.db.ExecContext(ctx, query, args...)
	end()

	return res, err
}

// MSSQLTx implements the Transaction interface.
type MSSQLTx struct {
	db *sql.DB
	tx *sql.Tx
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (m *MSSQLTx) Ping(ctx context.Context) error {
	if m == nil {
		return ErrEmptyObject
	}

	// This will choose the default recorder chosen during setup. If metrics.MetricsRecorder is never changed,
	// this will default to the noop recorder.
	r := metrics.GetRecorder(ctx)

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	var result []string
	end := r.DatabaseSegment("mssql", "select getdate()")
	rows, err := m.tx.QueryContext(ctx, "select getdate()")
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

// Shutdown has no context during a transaction currently, but is provided to implement the Database interface.
func (*MSSQLTx) Shutdown(context.Context) error {
	return nil
}

// Stats has no context during a transaction currently, but is provided to implement the Database interface.
func (*MSSQLTx) Stats(context.Context) sql.DBStats {
	return sql.DBStats{}
}

// Fetch provides a simple query-and-get operation as part of a transaction. We will run your query and fill your container.
func (m *MSSQLTx) Fetch(ctx context.Context, query string, container interface{}, args ...interface{}) error {
	return m.FetchWithMetrics(ctx, &metrics.NoOp{}, query, container, args...)
}

// FetchWithMetrics provides a simple query-and-get operation as part of a transaction. We will run your query and fill your container.
func (m *MSSQLTx) FetchWithMetrics(ctx context.Context, r metrics.Recorder, query string, container interface{}, args ...interface{}) error {
	if m == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	rows, err := m.tx.QueryContext(ctx, query, args...)
	end()
	if err != nil {
		if err.Error() == "pq: canceling statement due to user request" {
			return fmt.Errorf("%w: %v", ctx.Err(), err)
		}

		return err
	}

	defer rows.Close()

	end = r.Segment("GODB::FetchWithMetrics::UnmarshalWithMetrics")
	err = UnmarshalWithMetrics(r, rows, &container)
	end()
	return err
}

// Exec provides a simple no-return-expected query as part of a transaction. We will run your query and send you on your way.
// Great for inserts and updates.
func (m *MSSQLTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return m.ExecWithMetrics(ctx, &metrics.NoOp{}, query, args...)
}

// ExecWithMetrics provides a simple no-return-expected query as part of a transaction. We will run your query and send you on your way.
// Great for inserts and updates.
func (m *MSSQLTx) ExecWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) (sql.Result, error) {
	if m == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	res, err := m.tx.ExecContext(ctx, query, args...)
	end()

	if err != nil {
		if err.Error() == "pq: canceling statement due to user request" {
			return nil, fmt.Errorf("%w: %v", ctx.Err(), err)
		}

		return nil, err
	}

	return res, nil
}

// FetchJSON provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (m *MSSQLTx) FetchJSON(ctx context.Context, query string, args ...interface{}) ([]byte, error) {
	return m.FetchJSONWithMetrics(ctx, &metrics.NoOp{}, query, args...)
}

// FetchJSONWithMetrics provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (m *MSSQLTx) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) ([]byte, error) {
	if m == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("mssql", query, args...)
	rows, err := m.tx.QueryContext(ctx, query, args...)
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

// Commit commits the transaction
func (m *MSSQLTx) Commit() error {
	return m.tx.Commit()
}

// Rollback commits the transaction
func (m *MSSQLTx) Rollback() error {
	return m.tx.Rollback()
}

package godb

import (
	"context"
	"errors"
	"fmt"

	"database/sql"

	"github.com/btm6084/utilities/metrics"
	"github.com/btm6084/utilities/stack"
	log "github.com/sirupsen/logrus"

	// Causes side effects in database/sql and allows us to connect to mysql
	_ "github.com/go-sql-driver/mysql"
)

// MySQLDatastore is an implementation of MySQLSQL datastore for golang.
type MySQLDatastore struct {
	db *sql.DB
}

// NewMySQLDatastore configures and returns a usable MySQLDatastore from parameters.
func NewMySQLDatastore(user, pass, dbName, host, port string, maxOpen, maxIdle int) *MySQLDatastore {
	return NewMySQLDatastoreCS(fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, dbName), maxOpen, maxIdle)

}

// NewMySQLDatastoreCS configures and returns a usable MySQLDatastore from a connect string.
func NewMySQLDatastoreCS(connectString string, maxOpen, maxIdle int) *MySQLDatastore {
	db, err := sql.Open("mysql", connectString)
	if err != nil {
		log.WithFields(stack.TraceFields()).Println(err)
		return nil
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	store := &MySQLDatastore{db}

	err = store.Ping(context.Background())
	if err != nil {
		log.WithFields(stack.TraceFields()).Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (m *MySQLDatastore) Ping(ctx context.Context) error {
	if m == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	var result []string
	rows, err := m.db.QueryContext(ctx, "SELECT VERSION()")
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
func (m *MySQLDatastore) Shutdown(context.Context) error {
	if m != nil && m == nil {
		return ErrEmptyObject
	}

	if m.db == nil {
		return fmt.Errorf("no valid database")
	}

	m.db.Close()
	return nil
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (m *MySQLDatastore) Fetch(ctx context.Context, query string, container interface{}, args ...interface{}) error {
	if m == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	defer rows.Close()
	err = Unmarshal(rows, &container)
	return err
}

// FetchWithMetrics provides a simple query-and-get operation. We will run your query and fill your container.
func (m *MySQLDatastore) FetchWithMetrics(ctx context.Context, r metrics.Recorder, query string, container interface{}, args ...interface{}) error {
	if m == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("mysql", query, args...)
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
func (m *MySQLDatastore) FetchJSON(ctx context.Context, query string, args ...interface{}) ([]byte, error) {
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
func (m *MySQLDatastore) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) ([]byte, error) {
	if m == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("mysql", query, args...)
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
func (m *MySQLDatastore) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
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
func (m *MySQLDatastore) ExecWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) (sql.Result, error) {
	if m == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("mysql", query, args...)
	res, err := m.db.ExecContext(ctx, query, args...)
	end()

	return res, err
}

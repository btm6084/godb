package godb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/btm6084/utilities/metrics"
	"github.com/btm6084/utilities/stack"
	log "github.com/sirupsen/logrus"

	// Causes side effects in database/sql and allows us to connect to postgres.
	_ "github.com/lib/pq"
)

// PostgresDatastore is an implementation of PostgresSQL datastore for golang.
type PostgresDatastore struct {
	db *sql.DB
}

// NewPostgresDatastore configures and returns a usable PostgresDatastore from parameters.
func NewPostgresDatastore(user, pass, dbName, host, port string, maxOpen, maxIdle int) *PostgresDatastore {
	return NewPostgresDatastoreCS(fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable", user, pass, dbName, host, port), maxOpen, maxIdle)

}

// NewPostgresDatastoreCS configures and returns a usable PostgresDatastore from a connect string.
func NewPostgresDatastoreCS(connectString string, maxOpen, maxIdle int) *PostgresDatastore {
	db, err := sql.Open("postgres", connectString)
	if err != nil {
		log.WithFields(stack.TraceFields()).Println(err)
		return nil
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	store := &PostgresDatastore{db}

	err = store.Ping(context.Background())
	if err != nil {
		log.WithFields(stack.TraceFields()).Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (p *PostgresDatastore) Ping(ctx context.Context) error {
	if p == nil {
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
	end := r.DatabaseSegment("mssql", "select now()")
	rows, err := p.db.QueryContext(ctx, "select now()")
	end()
	if err != nil {
		if err.Error() == "pq: canceling statement due to user request" {
			return fmt.Errorf("%w: %v", ctx.Err(), err)
		}

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
func (p *PostgresDatastore) Shutdown(context.Context) error {
	if p != nil && p.db != nil {
		p.db.Close()
	}
	return nil
}

// Stats returns statistics about the current DB connection.
func (p *PostgresDatastore) Stats(context.Context) sql.DBStats {
	if p != nil && p.db != nil {
		return p.db.Stats()
	}
	return sql.DBStats{}
}

// Begin starts a single transaction. You MUST call Transaction.Rollback, or Transaction.Commit after calling Begin, or you WILL
// leak memory.
// It is safe to defer Transaction.Rollback immediately, even if you don't intend to rollback.
// Once you Commit, Rollback becomes a no-op.
func (p *PostgresDatastore) BeginTx(ctx context.Context) (Transaction, error) {
	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	return &PostgresTx{db: p.db, tx: tx}, nil
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (p *PostgresDatastore) Fetch(ctx context.Context, query string, container interface{}, args ...interface{}) error {
	if p == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		if err.Error() == "pq: canceling statement due to user request" {
			return fmt.Errorf("%w: %v", ctx.Err(), err)
		}

		return err
	}

	defer rows.Close()
	err = Unmarshal(rows, &container)
	return err
}

// FetchWithMetrics provides a simple query-and-get operation. We will run your query and fill your container.
func (p *PostgresDatastore) FetchWithMetrics(ctx context.Context, r metrics.Recorder, query string, container interface{}, args ...interface{}) error {
	if p == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	rows, err := p.db.QueryContext(ctx, query, args...)
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

// FetchJSON provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (p *PostgresDatastore) FetchJSON(ctx context.Context, query string, args ...interface{}) ([]byte, error) {
	if p == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		if err.Error() == "pq: canceling statement due to user request" {
			return nil, fmt.Errorf("%w: %v", ctx.Err(), err)
		}

		return nil, err
	}

	defer rows.Close()

	return ToJSON(rows)
}

// FetchJSONWithMetrics provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (p *PostgresDatastore) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) ([]byte, error) {
	if p == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	rows, err := p.db.QueryContext(ctx, query, args...)
	end()
	if err != nil {
		if err.Error() == "pq: canceling statement due to user request" {
			return nil, fmt.Errorf("%w: %v", ctx.Err(), err)
		}

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
func (p *PostgresDatastore) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return p.ExecWithMetrics(ctx, &metrics.NoOp{}, query, args...)
}

// ExecWithMetrics provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (p *PostgresDatastore) ExecWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) (sql.Result, error) {
	if p == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	res, err := p.db.ExecContext(ctx, query, args...)
	end()

	if err != nil {
		if err.Error() == "pq: canceling statement due to user request" {
			return nil, fmt.Errorf("%w: %v", ctx.Err(), err)
		}

		return nil, err
	}

	return res, nil
}

// PostgresTx implements the Transaction interface.
type PostgresTx struct {
	db *sql.DB
	tx *sql.Tx
}

// Fetch provides a simple query-and-get operation as part of a transaction. We will run your query and fill your container.
func (p *PostgresTx) Fetch(ctx context.Context, query string, container interface{}, args ...interface{}) error {
	return p.FetchWithMetrics(ctx, &metrics.NoOp{}, query, container, args...)
}

// FetchWithMetrics provides a simple query-and-get operation as part of a transaction. We will run your query and fill your container.
func (p *PostgresTx) FetchWithMetrics(ctx context.Context, r metrics.Recorder, query string, container interface{}, args ...interface{}) error {
	if p == nil {
		return ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	rows, err := p.tx.QueryContext(ctx, query, args...)
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
func (p *PostgresTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return p.ExecWithMetrics(ctx, &metrics.NoOp{}, query, args...)
}

// ExecWithMetrics provides a simple no-return-expected query as part of a transaction. We will run your query and send you on your way.
// Great for inserts and updates.
func (p *PostgresTx) ExecWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) (sql.Result, error) {
	if p == nil {
		return nil, ErrEmptyObject
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	res, err := p.tx.ExecContext(ctx, query, args...)
	end()

	if err != nil {
		if err.Error() == "pq: canceling statement due to user request" {
			return nil, fmt.Errorf("%w: %v", ctx.Err(), err)
		}

		return nil, err
	}

	return res, nil
}

// Commit commits the transaction
func (p *PostgresTx) Commit() error {
	return p.tx.Commit()
}

// Rollback commits the transaction
func (p *PostgresTx) Rollback() error {
	return p.tx.Rollback()
}

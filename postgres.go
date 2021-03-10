package godb

import (
	"context"
	"errors"
	"fmt"
	"log"

	"database/sql"

	// Causes side effects in database/sql and allows us to connect to postgres.
	"github.com/btm6084/utilities/metrics"
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
		log.Println(err)
		return nil
	}

	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	store := &PostgresDatastore{db}

	err = store.Ping(context.Background())
	if err != nil {
		log.Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (p *PostgresDatastore) Ping(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	var result []string
	rows, err := p.db.QueryContext(ctx, "select now()")
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
func (p *PostgresDatastore) Shutdown(context.Context) error {
	if p.db != nil {
		p.db.Close()
	}
	return nil
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (p *PostgresDatastore) Fetch(ctx context.Context, query string, container interface{}, args ...interface{}) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}

	defer rows.Close()
	err = Unmarshal(rows, &container)
	return err
}

// FetchWithMetrics provides a simple query-and-get operation. We will run your query and fill your container.
func (p *PostgresDatastore) FetchWithMetrics(ctx context.Context, r metrics.Recorder, query string, container interface{}, args ...interface{}) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	rows, err := p.db.QueryContext(ctx, query, args...)
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
func (p *PostgresDatastore) FetchJSON(ctx context.Context, query string, args ...interface{}) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return ToJSON(rows)
}

// FetchJSONWithMetrics provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (p *PostgresDatastore) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) ([]byte, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	rows, err := p.db.QueryContext(ctx, query, args...)
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
func (p *PostgresDatastore) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	return p.db.ExecContext(ctx, query, args...)
}

// ExecWithMetrics provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (p *PostgresDatastore) ExecWithMetrics(ctx context.Context, r metrics.Recorder, query string, args ...interface{}) (sql.Result, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, QueryLimit)
		defer cancel()
	}

	end := r.DatabaseSegment("postgres", query, args...)
	res, err := p.db.ExecContext(ctx, query, args...)
	end()

	return res, err
}

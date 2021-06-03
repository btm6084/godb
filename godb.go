package godb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/btm6084/utilities/metrics"
	"github.com/go-test/deep"
	"github.com/stretchr/testify/assert"
)

// Interface Assertions
var (
	_ TransactionDB = (*PostgresDatastore)(nil)
	_ Transaction   = (*PostgresTx)(nil)

	_ Database = (*MySQLDatastore)(nil)
	_ Database = (*PostgresDatastore)(nil)
	_ Database = (*MSSQLDatastore)(nil)
	_ Database = (*SQLiteDatastore)(nil)

	_ Fetcher = (*MySQLDatastore)(nil)
	_ Fetcher = (*PostgresDatastore)(nil)
	_ Fetcher = (*MSSQLDatastore)(nil)
	_ Fetcher = (*SQLiteDatastore)(nil)
	_ Fetcher = (*JSONApi)(nil)

	_ JSONFetcher = (*MySQLDatastore)(nil)
	_ JSONFetcher = (*PostgresDatastore)(nil)
	_ JSONFetcher = (*MSSQLDatastore)(nil)
	_ JSONFetcher = (*SQLiteDatastore)(nil)
	_ JSONFetcher = (*JSONApi)(nil)

	_ Executer = (*MySQLDatastore)(nil)
	_ Executer = (*PostgresDatastore)(nil)
	_ Executer = (*MSSQLDatastore)(nil)
	_ Executer = (*SQLiteDatastore)(nil)
)

var (
	// QueryLimit is a hard timeout on the amount of time a query is allowed to run.
	// QueryLimit is exported so that an application can adjust it to fit their needs.
	QueryLimit = 5 * time.Minute

	ErrEmptyObject = errors.New("godb empty object")
)

// Database implements an interface for interacting with a database.
type Database interface {
	Shutdown(context.Context) error

	pinger
	executer
	fetcher
	jsonFetcher
}

type TransactionDB interface {
	BeginTx(context.Context) (Transaction, error)

	Database
}

type Transaction interface {
	Commit() error
	Rollback() error

	fetcher
	executer
}

type Fetcher interface {
	fetcher
	pinger
}

type JSONFetcher interface {
	jsonFetcher
	pinger
}

type Executer interface {
	executer
	pinger
}

type fetcher interface {
	Fetch(context.Context, string, interface{}, ...interface{}) error
	FetchWithMetrics(context.Context, metrics.Recorder, string, interface{}, ...interface{}) error
}

type jsonFetcher interface {
	FetchJSON(context.Context, string, ...interface{}) ([]byte, error)
	FetchJSONWithMetrics(context.Context, metrics.Recorder, string, ...interface{}) ([]byte, error)
}

type executer interface {
	Exec(context.Context, string, ...interface{}) (sql.Result, error)
	ExecWithMetrics(context.Context, metrics.Recorder, string, ...interface{}) (sql.Result, error)
}

type pinger interface {
	Ping(context.Context) error
}

func assertDeepEqual(t *testing.T, a, b interface{}) bool {
	if !assert.ObjectsAreEqual(a, b) {
		diff := strings.Join(deep.Equal(a, b), "\n\t")
		assert.FailNow(t, fmt.Sprintf("Diff:\n\t%s", diff))

		return false
	}

	return true
}

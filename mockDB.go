package godb

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/btm6084/gojson"
	"github.com/stretchr/testify/assert"
	"github.com/btm6084/utilities/metrics"
)

var _ Database = &MockDB{}

// MockDB implements the Database interface and allows for database mocking.
type MockDB struct {
	t *testing.T

	FetchPointer  int
	FetchExpected []DBResult
	FetchCount    int

	FetchJSONPointer  int
	FetchJSONExpected []DBResult
	FetchJSONCount    int

	ExecPointer  int
	ExecExpected []DBResult
	ExecCount    int
}

// DBResult allows Exec/Fetch responses to be crafted.
type DBResult struct {
	Query   string
	Args    []interface{}
	Content []byte
	Error   error
	Result  SQLResult
}

// SQLResult allows Exec responses to be crafted.
type SQLResult struct {
	Affected    int64
	AffectedErr error
}

// LastInsertId returns the id of the last inserted row.
func (r *SQLResult) LastInsertId() (int64, error) { return 0, nil }

// RowsAffected returns the number of rows affected by an exec query.
func (r *SQLResult) RowsAffected() (int64, error) { return r.Affected, r.AffectedErr }

// NewMockDB returns a ready to use MockDB struct.
func NewMockDB(t *testing.T) *MockDB {
	return &MockDB{t: t}
}

// AssertNoCalls asserts that there were no Fetch or Exec calls.
func (db *MockDB) AssertNoCalls() {
	assert.Zero(db.t, db.FetchCount, "No Fetches Expected")
	assert.Zero(db.t, db.ExecCount, "No Execs Expected")
}

// OnConsecutiveFetch returns the next defined value each time one of the fetch functions is called.
func (db *MockDB) OnConsecutiveFetch(fc []DBResult) {
	db.FetchExpected = fc
}

// Ping satisfies the Database interface.
func (db *MockDB) Ping(context.Context) error {
	return nil
}

// Shutdown satisfies the Database interface.
func (db *MockDB) Shutdown(context.Context) error {
	return nil
}

// FetchWithMetrics mocks FetchWithMetrics by simply ignoring the metrics during the unittest.
// This allows FetchWithMetrics to work exactly as Fetch does during a unit test.
func (db *MockDB) FetchWithMetrics(ctx context.Context, r metrics.Recorder, q string, c interface{}, args ...interface{}) error {
	return db.Fetch(ctx, q, c, args...)
}

// Fetch allows for mocking the response from a fetch request.
func (db *MockDB) Fetch(ctx context.Context, q string, c interface{}, args ...interface{}) error {
	db.FetchCount++

	if !assert.True(db.t, len(db.FetchExpected) > 0, "No FetchExpected Defined") {
		db.t.FailNow()
	}

	// We repeat the final registered Fetch mock if we run out.
	if db.FetchPointer >= len(db.FetchExpected) {
		fmt.Print("\nMore Fetch Calls than Expected\n\n")
		db.t.FailNow()
	}

	fetch := db.FetchExpected[db.FetchPointer]
	db.FetchPointer++

	if !assert.Equal(db.t, fetch.Query, q) {
		db.t.FailNow()
	}

	assertDeepEqual(db.t, fetch.Args, args)

	if fetch.Error != nil {
		return fetch.Error
	}

	err := gojson.Unmarshal(fetch.Content, c)
	assert.Nil(db.t, err)

	return nil
}

// ExecWithMetrics mocks ExecWithMetrics by simply ignoring the metrics during the unittest.
// This allows ExecWithMetrics to work exactly as Exec does during a unit test.
func (db *MockDB) ExecWithMetrics(ctx context.Context, r metrics.Recorder, q string, args ...interface{}) (sql.Result, error) {
	return db.Exec(ctx, q, args...)
}

// Exec allows for mocking the response from an exec request.
func (db *MockDB) Exec(ctx context.Context, q string, args ...interface{}) (sql.Result, error) {
	db.ExecCount++

	if !assert.True(db.t, len(db.ExecExpected) > 0, "No ExecExpected Defined") {
		db.t.FailNow()
	}

	// We repeat the final registered Exec mock if we run out.
	if db.ExecPointer >= len(db.ExecExpected) {
		fmt.Print("\nMore Exec Calls than Expected\n\n")
		db.t.FailNow()
	}

	exec := db.ExecExpected[db.ExecPointer]
	db.ExecPointer++

	if !assert.Equal(db.t, exec.Query, q) {
		db.t.FailNow()
	}

	assertDeepEqual(db.t, exec.Args, args)

	if exec.Error != nil {
		return nil, exec.Error
	}

	return &exec.Result, nil
}

// FetchJSONWithMetrics mocks FetchJSONWithMetrics by simply ignoring the metrics during the unittest.
// This allows FetchJSONWithMetrics to work exactly as FetchJSON does during a unit test.
func (db *MockDB) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, q string, args ...interface{}) ([]byte, error) {
	return db.FetchJSON(ctx, q, args...)
}

// FetchJSON allows for mocking the response from a fetch request.
func (db *MockDB) FetchJSON(ctx context.Context, q string, args ...interface{}) ([]byte, error) {
	db.FetchJSONCount++

	if !assert.True(db.t, len(db.FetchJSONExpected) > 0, "No FetchJSONExpected Defined") {
		db.t.FailNow()
	}

	// We repeat the final registered FetchJSON mock if we run out.
	if db.FetchJSONPointer >= len(db.FetchJSONExpected) {
		fmt.Print("\nMore FetchJSON Calls than Expected\n\n")
		db.t.FailNow()
	}

	fetch := db.FetchJSONExpected[db.FetchJSONPointer]
	db.FetchJSONPointer++

	if !assert.Equal(db.t, fetch.Query, q) {
		db.t.FailNow()
	}

	assertDeepEqual(db.t, fetch.Args, args)

	if fetch.Error != nil {
		return nil, fetch.Error
	}

	return fetch.Content, nil
}

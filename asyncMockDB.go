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

var _ Database = &AsyncMockDB{}

// AsyncMockDB implements the Database interface and allows for database mocking.
// AsyncMockDB checks THAT a query executes, but does not say that it happens in any order.
// If you need to assert that your queries happen in order, use MockDB
type AsyncMockDB struct {
	t *testing.T

	FetchPointer int
	FetchCount   int

	FetchJSONPointer int
	FetchJSONCount   int

	ExecPointer int
	ExecCount   int

	CallCount int
	Expected  []DBResult
}

// NewAsyncMockDB returns a ready to use AsyncMockDB struct.
func NewAsyncMockDB(t *testing.T) *AsyncMockDB {
	return &AsyncMockDB{t: t}
}

// AssertNoCalls asserts that there were no Fetch or Exec calls.
func (db *AsyncMockDB) AssertNoCalls() {
	assert.Zero(db.t, db.FetchCount, "No Fetches Expected")
	assert.Zero(db.t, db.ExecCount, "No Execs Expected")
}

// Ping satisfies the Database interface.
func (db *AsyncMockDB) Ping(ctx context.Context) error {
	return nil
}

// Shutdown satisfies the Database interface.
func (db *AsyncMockDB) Shutdown(ctx context.Context) error {
	return nil
}

// FetchWithMetrics mocks FetchWithMetrics by simply ignoring the metrics during the unittest.
// This allows FetchWithMetrics to work exactly as Fetch does during a unit test.
func (db *AsyncMockDB) FetchWithMetrics(ctx context.Context, r metrics.Recorder, q string, c interface{}, args ...interface{}) error {
	return db.Fetch(ctx, q, c, args...)
}

// Fetch allows for mocking the response from a fetch request.
func (db *AsyncMockDB) Fetch(ctx context.Context, q string, c interface{}, args ...interface{}) error {
	db.CallCount++
	db.FetchCount++

	if !assert.True(db.t, len(db.Expected) > 0, "No Expected Results Defined") {
		db.t.FailNow()
	}

	// We repeat the final registered Fetch mock if we run out.
	if db.FetchCount > len(db.Expected) {
		fmt.Print("\nMore Fetch Calls than Expected\n\n")
		db.t.FailNow()
	}

	var fetch DBResult
	for _, f := range db.Expected {
		if f.Query == q {
			fetch = f
			break
		}
	}

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
func (db *AsyncMockDB) ExecWithMetrics(ctx context.Context, r metrics.Recorder, q string, args ...interface{}) (sql.Result, error) {
	return db.Exec(ctx, q, args...)
}

// Exec allows for mocking the response from an exec request.
func (db *AsyncMockDB) Exec(ctx context.Context, q string, args ...interface{}) (sql.Result, error) {
	db.CallCount++
	db.ExecCount++

	if !assert.True(db.t, len(db.Expected) > 0, "No Expected Defined") {
		db.t.FailNow()
	}

	// We repeat the final registered Exec mock if we run out.
	if db.ExecCount > len(db.Expected) {
		fmt.Print("\nMore Exec Calls than Expected\n\n")
		db.t.FailNow()
	}

	var exec DBResult
	for _, e := range db.Expected {
		if e.Query == q {
			exec = e
			break
		}
	}

	if exec.Query == "" || !assert.Equal(db.t, exec.Query, q) {
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
func (db *AsyncMockDB) FetchJSONWithMetrics(ctx context.Context, r metrics.Recorder, q string, args ...interface{}) ([]byte, error) {
	return db.FetchJSON(ctx, q, args...)
}

// FetchJSON allows for mocking the response from a fetch request.
func (db *AsyncMockDB) FetchJSON(ctx context.Context, q string, args ...interface{}) ([]byte, error) {
	db.CallCount++
	db.FetchJSONCount++

	if !assert.True(db.t, len(db.Expected) > 0, "No Expected Defined") {
		db.t.FailNow()
	}

	// We repeat the final registered FetchJSON mock if we run out.
	if db.FetchJSONPointer >= len(db.Expected) {
		fmt.Print("\nMore FetchJSON Calls than Expected\n\n")
		db.t.FailNow()
	}

	fetch := db.Expected[db.FetchJSONPointer]
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

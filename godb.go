package godb

import "database/sql"

// Database implements an interface for interacting with a database.
type Database interface {
	Ping() error
	Shutdown() error
	Query(string, ...interface{}) (Rows, error)
	Fetch(string, interface{}, ...interface{}) error
	Exec(string, ...interface{}) (sql.Result, error)
}

// Rows is an interface that can be satisfied by sql.Rows
type Rows interface {
	Columns() ([]string, error)
	Next() bool
	Close() error
	Scan(...interface{}) error
}

package godb

import (
	"database/sql"
)

// Database implements an interface for interacting with a database.
type Database interface {
	Ping() error
	Shutdown() error
	Fetch(string, interface{}, ...interface{}) error
	Exec(string, ...interface{}) (sql.Result, error)
	Query(string, ...interface{}) (*sql.Rows, error)
}

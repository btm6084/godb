package godb

import (
	"errors"
	"log"

	"database/sql"

	// Causes side effects in database/sql and allows us to connect to sqlserver.
	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDatastore is an implementation of SQLiteSQL datastore for golang.
type SQLiteDatastore struct {
	db *sql.DB
}

// NewSQLiteDatastore configures and returns a usable SQLiteDatastore
func NewSQLiteDatastore(file string) *SQLiteDatastore {
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		log.Println(err)
		return nil
	}

	store := &SQLiteDatastore{db}

	err = store.Ping()
	if err != nil {
		log.Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (p *SQLiteDatastore) Ping() error {
	var result []string
	rows, err := p.db.Query("SELECT strftime('%s', 'now');")
	defer rows.Close()
	Unmarshal(rows, &result)

	if len(result) < 1 {
		return errors.New("Ping Failed")
	}

	return err
}

// Shutdown performs any closing operations. Best called as deferred from main after the datastore is initialized.
func (p *SQLiteDatastore) Shutdown() error {
	p.db.Close()
	return nil
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (p *SQLiteDatastore) Fetch(query string, container interface{}, args ...interface{}) error {
	rows, err := p.db.Query(query, args...)
	if err != nil {
		return err
	}

	err = Unmarshal(rows, &container)
	return err
}

// Exec provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (p *SQLiteDatastore) Exec(query string, args ...interface{}) (sql.Result, error) {
	return p.db.Exec(query, args...)
}

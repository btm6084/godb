package godb

import (
	"errors"
	"fmt"
	"log"

	"database/sql"

	// Causes side effects in database/sql and allows us to connect to postgres.
	_ "github.com/lib/pq"
)

// PostgresDatastore is an implementation of PostgresSQL datastore for golang.
type PostgresDatastore struct {
	db *sql.DB
}

// NewPostgresDatastore configures and returns a usable PostgresDatastore
func NewPostgresDatastore(user, pass, dbName, host, port string) *PostgresDatastore {
	connectString := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable", user, pass, dbName, host, port)
	db, err := sql.Open("postgres", connectString)
	if err != nil {
		log.Println(err)
		return nil
	}

	store := &PostgresDatastore{db}

	err = store.Ping()
	if err != nil {
		log.Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (p *PostgresDatastore) Ping() error {
	var result []string
	rows, err := p.db.Query("select now()")
	defer rows.Close()
	Unmarshal(rows, &result)

	if len(result) < 1 {
		return errors.New("Ping Failed")
	}

	return err
}

// Shutdown performs any closing operations. Best called as deferred from main after the datastore is initialized.
func (p *PostgresDatastore) Shutdown() error {
	p.db.Close()
	return nil
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (p *PostgresDatastore) Fetch(query string, container interface{}, args ...interface{}) error {
	rows, err := p.db.Query(query, args...)
	if err != nil {
		return err
	}

	err = Unmarshal(rows, &container)
	return err
}

// Exec provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (p *PostgresDatastore) Exec(query string, args ...interface{}) (sql.Result, error) {
	return p.db.Exec(query, args...)
}
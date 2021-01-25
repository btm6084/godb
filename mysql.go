package godb

import (
	"errors"
	"fmt"
	"log"

	"database/sql"

	// Causes side effects in database/sql and allows us to connect to postgres.
	_ "github.com/go-sql-driver/mysql"
)

// MySQLDatastore is an implementation of MySQLSQL datastore for golang.
type MySQLDatastore struct {
	db *sql.DB
}

// NewMySQLDatastore configures and returns a usable MySQLDatastore from parameters.
func NewMySQLDatastore(user, pass, dbName, host, port string) *MySQLDatastore {
	return NewMySQLDatastoreCS(fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, dbName))

}

// NewMySQLDatastoreCS configures and returns a usable MySQLDatastore from a connect string.
func NewMySQLDatastoreCS(connectString string) *MySQLDatastore {
	db, err := sql.Open("mysql", connectString)
	if err != nil {
		log.Println(err)
		return nil
	}

	store := &MySQLDatastore{db}

	err = store.Ping()
	if err != nil {
		log.Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (p *MySQLDatastore) Ping() error {
	if p.db == nil {
		return fmt.Errorf("no valid database")
	}

	var result []string
	rows, err := p.db.Query("SELECT VERSION()")
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
func (p *MySQLDatastore) Shutdown() error {
	if p.db == nil {
		return fmt.Errorf("no valid database")
	}

	p.db.Close()
	return nil
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (p *MySQLDatastore) Fetch(query string, container interface{}, args ...interface{}) error {
	if p.db == nil {
		return fmt.Errorf("no valid database")
	}

	rows, err := p.db.Query(query, args...)
	if err != nil {
		return err
	}

	defer rows.Close()

	err = Unmarshal(rows, &container)
	return err
}

// FetchJSON provides a simple query-and-get operation. We will run your query and give you back the JSON representing your result set.
func (p *MySQLDatastore) FetchJSON(query string, args ...interface{}) ([]byte, error) {
	if p.db == nil {
		return nil, fmt.Errorf("no valid database")
	}

	rows, err := p.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return ToJSON(rows)
}

// Query provides a simple query operation. You will receive the raw sql.Rows object.
func (p *MySQLDatastore) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if p.db == nil {
		return nil, fmt.Errorf("no valid database")
	}

	return p.db.Query(query, args...)
}

// Exec provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (p *MySQLDatastore) Exec(query string, args ...interface{}) (sql.Result, error) {
	if p.db == nil {
		return nil, fmt.Errorf("no valid database")
	}

	return p.db.Exec(query, args...)
}

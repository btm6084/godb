package godb

import (
	"errors"
	"fmt"
	"log"
	"net/url"

	"database/sql"

	// Causes side effects in database/sql and allows us to connect to sqlserver.
	_ "github.com/denisenkom/go-mssqldb"
)

// MSSQLDatastore is an implementation of MSSQLDatastore datastore for golang.
type MSSQLDatastore struct {
	db *sql.DB
}

// NewMSSQLDatastore configures and returns a usable MSSQLDatastore from parameters.
func NewMSSQLDatastore(user, pass, dbName, host, port, appname string) *MSSQLDatastore {

	query := url.Values{}
	query.Add("database", dbName)
	query.Add("app name", appname)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(user, pass),
		Host:     fmt.Sprintf("%s:%s", host, port),
		RawQuery: query.Encode(),
	}

	return NewMSSQLDatastoreCS(u.String())
}

// NewMSSQLDatastoreCS configures and returns a usable MSSQLDatastore from a connect string.
func NewMSSQLDatastoreCS(connectString string) *MSSQLDatastore {
	db, err := sql.Open("sqlserver", connectString)
	if err != nil {
		log.Println(err)
		return nil
	}

	store := &MSSQLDatastore{db}

	err = store.Ping()
	if err != nil {
		log.Println(err)
		return nil
	}

	return store
}

// Ping sends a ping to the server and returns an error if it cannot connect.
func (m *MSSQLDatastore) Ping() error {
	var result []string
	rows, err := m.db.Query("select getdate()")
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
func (m *MSSQLDatastore) Shutdown() error {
	m.db.Close()
	return nil
}

// Fetch provides a simple query-and-get operation. We will run your query and fill your container.
func (m *MSSQLDatastore) Fetch(query string, container interface{}, args ...interface{}) error {
	rows, err := m.db.Query(query, args...)
	if err != nil {
		return err
	}

	err = Unmarshal(rows, &container)
	return err
}

// Query provides a simple query operation. You will receive the raw sql.Rows object.
func (m *MSSQLDatastore) Query(query string, args ...interface{}) (Rows, error) {
	return m.db.Query(query, args...)
}

// Exec provides a simple no-return-expected query. We will run your query and send you on your way.
// Great for inserts and updates.
func (m *MSSQLDatastore) Exec(query string, args ...interface{}) (sql.Result, error) {
	return m.db.Exec(query, args...)
}

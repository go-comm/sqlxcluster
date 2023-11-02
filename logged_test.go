package sqlxcluster

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

type testDB struct {
	DB
}

func (db *testDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if strings.Index(query, "select") == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("no support " + query)
}

func TestLoggedQuery(t *testing.T) {
	var db DB = &testDB{}
	db = NewLoggedDB(db, false, nil)

	db.Query("select")
	db.Query("wrong")
}

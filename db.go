package sqlxcluster

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/jmoiron/sqlx"
)

var _ sql.Tx
var _ sql.DB
var _ DB = (*sqlx.DB)(nil)
var _ Tx = (*sqlx.Tx)(nil)

type Command interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row

	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	// NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

type DB interface {
	Driver() driver.Driver
	Conn(ctx context.Context) (*sql.Conn, error)
	Ping() error
	PingContext(ctx context.Context) error
	Close() error
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	SetConnMaxIdleTime(d time.Duration)
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() sql.DBStats

	DriverName() string
	Connx(ctx context.Context) (*sqlx.Conn, error)
	Beginx() (*sqlx.Tx, error)
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)

	Command
}

type Tx interface {
	DriverName() string
	Commit() error
	Rollback() error

	Command
}

func NewDB(db *sql.DB, driverName string) DB {
	return &wrappedDB{DB: sqlx.NewDb(db, driverName)}
}

type wrappedDB struct {
	*sqlx.DB
}

func (w *wrappedDB) Unwrap() *sql.DB {
	return w.DB.DB
}

func Begin(db DB) (tx Tx, err error) {
	tx, err = db.Beginx()
	if err != nil {
		return tx, err
	}
	if ldb, ok := db.(*loggedDB); ok {
		tx = NewLoggedTx(tx, ldb.color, ldb.out)
	}
	return tx, err
}

func BeginTx(db DB, ctx context.Context, opts *sql.TxOptions) (tx Tx, err error) {
	tx, err = db.BeginTxx(ctx, opts)
	if err != nil {
		return tx, err
	}
	if ldb, ok := db.(*loggedDB); ok {
		tx = NewLoggedTx(tx, ldb.color, ldb.out)
	}
	return tx, err
}

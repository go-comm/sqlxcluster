package sqlcluster

import (
	"context"
	"database/sql"
	"math/rand"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
)

var rn = rand.New(rand.NewSource(time.Now().UnixNano() * int64(os.Getpid())))

func newReader(db *sqlx.DB) *Reader {
	r := &Reader{DB: db}
	return r
}

type Reader struct {
	*sqlx.DB
}

func newWriter(db *sqlx.DB) *Writer {
	w := &Writer{DB: db}
	return w
}

type Writer struct {
	*sqlx.DB
}

func NewClusterDB(w *sql.DB, r []*sql.DB, driverName string) *ClusterDB {
	c := &ClusterDB{
		w: newWriter(sqlx.NewDb(w, driverName)),
	}
	for i := 0; i < len(r); i++ {
		c.r = append(c.r, newReader(sqlx.NewDb(r[i], driverName)))
	}
	return c
}

type ClusterDB struct {
	w *Writer
	r []*Reader
}

func (c *ClusterDB) R() *sqlx.DB {
	return c.DB(true)
}

func (c *ClusterDB) W() *sqlx.DB {
	return c.DB(false)
}

func (c *ClusterDB) Begin() (*sql.Tx, error) {
	return c.DB(false).Begin()
}

func (c *ClusterDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return c.DB(false).BeginTx(ctx, opts)
}

func (c *ClusterDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return c.DB(false).Exec(query, args)
}

func (c *ClusterDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.DB(false).ExecContext(ctx, query, args)
}

func (c *ClusterDB) Ping() error {
	return c.PingContext(context.TODO())
}

func (c *ClusterDB) PingContext(ctx context.Context) error {
	if err := c.w.DB.PingContext(ctx); err != nil {
		return err
	}
	for i := 0; i < len(c.r); i++ {
		if err := c.r[i].DB.PingContext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClusterDB) Prepare(query string) (*sql.Stmt, error) {
	return c.DB(false).Prepare(query)
}

func (c *ClusterDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return c.DB(false).PrepareContext(ctx, query)
}

func (c *ClusterDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB(true).Query(query, args)
}

func (c *ClusterDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB(true).QueryContext(ctx, query, args)
}

func (c *ClusterDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.DB(true).QueryRow(query, args)
}

func (c *ClusterDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.DB(true).QueryRowContext(ctx, query, args)
}

func (c *ClusterDB) Beginx() (*sqlx.Tx, error) {
	return c.DB(false).Beginx()
}

func (c *ClusterDB) BeginxTx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	return c.DB(false).BeginTxx(ctx, opts)
}

func (c *ClusterDB) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return c.DB(false).NamedExec(query, arg)
}

func (c *ClusterDB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return c.DB(false).NamedExecContext(ctx, query, arg)
}

func (c *ClusterDB) Get(dest interface{}, query string, args ...interface{}) error {
	return c.DB(true).Get(dest, query, args)
}

func (c *ClusterDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return c.DB(true).GetContext(ctx, dest, query, args)
}

func (c *ClusterDB) Select(dest interface{}, query string, args ...interface{}) error {
	return c.DB(true).Select(dest, query, args)
}

func (c *ClusterDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return c.DB(true).SelectContext(ctx, dest, query, args)
}

func (c *ClusterDB) PrepareNamed(dest interface{}, query string) (*sqlx.NamedStmt, error) {
	return c.DB(false).PrepareNamed(query)
}

func (c *ClusterDB) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	return c.DB(false).PrepareNamedContext(ctx, query)
}

func (c *ClusterDB) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return c.DB(true).Queryx(query, args)
}

func (c *ClusterDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return c.DB(true).QueryxContext(ctx, query, args)
}

func (c *ClusterDB) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	return c.DB(true).QueryRowx(query, args)
}

func (c *ClusterDB) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	return c.DB(true).QueryRowxContext(ctx, query, args)
}

func (c *ClusterDB) DB(readOnly bool) *sqlx.DB {
	if !readOnly {
		return c.w.DB
	}
	switch len(c.r) {
	case 0:
		return nil
	case 1:
		return c.r[0].DB
	default:
		return c.r[rn.Intn(len(c.r))].DB
	}
}

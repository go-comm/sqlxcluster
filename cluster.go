package sqlcluster

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

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

func NewClusterDB(w *sql.DB, r *sql.DB, driverName string) *ClusterDB {
	c := &ClusterDB{
		w: newWriter(sqlx.NewDb(w, driverName)),
		r: newReader(sqlx.NewDb(r, driverName)),
	}
	return c
}

type ClusterDB struct {
	w *Writer
	r *Reader
}

func (c *ClusterDB) R() *Reader {
	return c.r
}

func (c *ClusterDB) W() *Writer {
	return c.w
}

func (c *ClusterDB) Begin() (*sql.Tx, error) {
	return c.w.DB.Begin()
}

func (c *ClusterDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return c.w.DB.BeginTx(ctx, opts)
}

func (c *ClusterDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return c.w.DB.Exec(query, args)
}

func (c *ClusterDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.w.DB.ExecContext(ctx, query, args)
}

func (c *ClusterDB) Ping() error {
	if err := c.r.DB.Ping(); err != nil {
		return err
	}
	return c.w.DB.Ping()
}

func (c *ClusterDB) PingContext(ctx context.Context) error {
	if err := c.r.DB.PingContext(ctx); err != nil {
		return err
	}
	return c.w.DB.PingContext(ctx)
}

func (c *ClusterDB) Prepare(query string) (*sql.Stmt, error) {
	return c.w.DB.Prepare(query)
}

func (c *ClusterDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return c.w.DB.PrepareContext(ctx, query)
}

func (c *ClusterDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.r.DB.Query(query, args)
}

func (c *ClusterDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.r.DB.QueryContext(ctx, query, args)
}

func (c *ClusterDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.r.DB.QueryRow(query, args)
}

func (c *ClusterDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.r.DB.QueryRowContext(ctx, query, args)
}

func (c *ClusterDB) Beginx() (*sqlx.Tx, error) {
	return c.w.DB.Beginx()
}

func (c *ClusterDB) BeginxTx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	return c.w.DB.BeginTxx(ctx, opts)
}

func (c *ClusterDB) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return c.w.DB.NamedExec(query, arg)
}

func (c *ClusterDB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	return c.w.DB.NamedExecContext(ctx, query, arg)
}

func (c *ClusterDB) Get(dest interface{}, query string, args ...interface{}) error {
	return c.r.DB.Get(dest, query, args)
}

func (c *ClusterDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return c.r.DB.GetContext(ctx, dest, query, args)
}

func (c *ClusterDB) Select(dest interface{}, query string, args ...interface{}) error {
	return c.r.DB.Select(dest, query, args)
}

func (c *ClusterDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return c.r.DB.SelectContext(ctx, dest, query, args)
}

func (c *ClusterDB) PrepareNamed(dest interface{}, query string) (*sqlx.NamedStmt, error) {
	return c.w.DB.PrepareNamed(query)
}

func (c *ClusterDB) PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error) {
	return c.w.DB.PrepareNamedContext(ctx, query)
}

func (c *ClusterDB) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return c.r.DB.Queryx(query, args)
}

func (c *ClusterDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return c.r.DB.QueryxContext(ctx, query, args)
}

func (c *ClusterDB) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	return c.r.DB.QueryRowx(query, args)
}

func (c *ClusterDB) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	return c.r.DB.QueryRowxContext(ctx, query, args)
}

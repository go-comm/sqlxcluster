package sqlxcluster

import (
	"context"
	"database/sql"
	"math/rand"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
)

var rn = rand.New(rand.NewSource(time.Now().UnixNano() * int64(os.Getpid())))

func WithName(name string) func(os *ClusterDB) {
	return func(os *ClusterDB) {
		os.name = name
	}
}

func WithEnableLog(enableLog bool) func(os *ClusterDB) {
	return func(os *ClusterDB) {
		os.enableLog = enableLog
	}
}

func NewClusterdb(w *sql.DB, r []*sql.DB, driverName string, opts ...func(os *ClusterDB)) *ClusterDB {
	c := &ClusterDB{
		DB: NewDB(w, driverName),
	}

	for _, opt := range opts {
		opt(c)
	}

	for _, e := range r {
		db := NewDB(e, driverName)
		if c.enableLog {
			db = NewLoggedDB(db)
		}
		c.r = append(c.r, db)
	}

	return c
}

var _ DB = (*ClusterDB)(nil)

type ClusterDB struct {
	DB             // write + read
	r         []DB // only read
	name      string
	enableLog bool
	meta      interface{}
}

func (c *ClusterDB) Name() string {
	return c.name
}

func (c *ClusterDB) SetName(name string) {
	c.name = name
}

func (c *ClusterDB) Meta() interface{} {
	return c.meta
}

func (c *ClusterDB) SetMeta(meta interface{}) {
	c.meta = meta
}

func (c *ClusterDB) R() DB {
	return c.db(true)
}

func (c *ClusterDB) W() DB {
	return c.db(false)
}

func (c *ClusterDB) Close() error {
	for _, e := range c.r {
		e.Close()
	}
	return c.DB.Close()
}

func (c *ClusterDB) Ping() error {
	return c.PingContext(context.TODO())
}

func (c *ClusterDB) PingContext(ctx context.Context) error {
	if err := c.DB.PingContext(ctx); err != nil {
		return err
	}
	for i := 0; i < len(c.r); i++ {
		if err := c.r[i].PingContext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClusterDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.db(true).Query(query, args...)
}

func (c *ClusterDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.db(true).QueryContext(ctx, query, args...)
}

func (c *ClusterDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return c.db(true).QueryRow(query, args...)
}

func (c *ClusterDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.db(true).QueryRowContext(ctx, query, args...)
}

func (c *ClusterDB) Get(dest interface{}, query string, args ...interface{}) error {
	return c.db(true).Get(dest, query, args...)
}

func (c *ClusterDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return c.db(true).GetContext(ctx, dest, query, args...)
}

func (c *ClusterDB) Select(dest interface{}, query string, args ...interface{}) error {
	return c.db(true).Select(dest, query, args...)
}

func (c *ClusterDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return c.db(true).SelectContext(ctx, dest, query, args...)
}

func (c *ClusterDB) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	return c.db(true).Queryx(query, args...)
}

func (c *ClusterDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	return c.db(true).QueryxContext(ctx, query, args...)
}

func (c *ClusterDB) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	return c.db(true).QueryRowx(query, args...)
}

func (c *ClusterDB) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	return c.db(true).QueryRowxContext(ctx, query, args...)
}

func (c *ClusterDB) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	return c.db(true).NamedQuery(query, arg)
}

func (c *ClusterDB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	// return c.db(true).NamedQueryContxt(ctx, query, arg)
	return c.db(true).NamedQuery(query, arg)
}

func (c *ClusterDB) db(readOnly bool) DB {
	if !readOnly {
		return c.DB
	}
	switch len(c.r) {
	case 0:
		return c.DB
	case 1:
		return c.r[0]
	default:
		return c.r[rn.Intn(len(c.r))]
	}
}

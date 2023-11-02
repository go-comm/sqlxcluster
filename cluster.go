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

type options struct {
	name      string
	enableLog bool
	color     bool
	out       func(b []byte) (int, error)
}

func WithName(name string) func(os *options) {
	return func(os *options) {
		os.name = name
	}
}

func WithEnableLog(enableLog bool) func(os *options) {
	return func(os *options) {
		os.enableLog = enableLog
	}
}

func WithColor(color bool) func(os *options) {
	return func(os *options) {
		os.color = color
	}
}

func WithOutput(out func(b []byte) (int, error)) func(os *options) {
	return func(os *options) {
		os.out = out
	}
}

func NewClusterDB(w *sql.DB, r []*sql.DB, driverName string, opts ...func(os *options)) *ClusterDB {
	var os options
	for _, opt := range opts {
		opt(&os)
	}
	c := &ClusterDB{
		DB: NewDB(w, driverName),
	}
	for _, e := range r {
		c.r = append(c.r, NewDB(e, driverName))
	}
	c.SetName(os.name)
	c.SetLog(os.enableLog, os.color, os.out)
	return c
}

var _ DB = (*ClusterDB)(nil)

type ClusterDB struct {
	DB             // write + read
	r         []DB // only read
	name      string
	meta      interface{}
	enableLog bool
	color     bool
	out       func(b []byte) (int, error)
}

func (c *ClusterDB) SetLog(enable bool, color bool, out func(b []byte) (int, error)) {
	if out == nil {
		out = defaultOut
	}
	c.enableLog = enable
	c.color = color
	c.out = out
	r := c.r
	if enable {
		c.DB = NewLoggedDB(c.DB, color, out)
		for i := 0; i < len(r); i++ {
			r[i] = NewLoggedDB(r[i], color, out)
		}
	} else {
		c.DB = unwrapLoggedDB(c.DB)
		for i := 0; i < len(r); i++ {
			r[i] = unwrapLoggedDB(r[i])
		}
	}
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

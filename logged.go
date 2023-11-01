package sqlxcluster

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	stdlog = log.New(os.Stderr, "", log.LstdFlags|log.Llongfile)

	defaultOutput = func(b []byte) (int, error) {
		stdlog.Output(5, string(b))
		return len(b), nil
	}
)

func NewLoggedDB(db DB) DB {
	return &loggedDB{DB: db}
}

func unwrapLoggedDB(db DB) DB {
	u, ok := db.(interface{ Unwrap() DB })
	if ok {
		return u.Unwrap()
	}
	return db
}

type loggedDB struct {
	DB
	out func([]byte) (int, error)
}

func (db *loggedDB) SetOutput(out func([]byte) (int, error)) {
	db.out = out
}

func (db *loggedDB) log(err0 error, elapsed time.Duration, query string, args ...interface{}) {
	// 2009/01/23 01:23:23 /a/b/c/d.go:23: error
	// [OK] [200ms] select * from user

	var b []byte

	if err0 != nil && err0 != sql.ErrNoRows {
		b = append(b, " \033[31m"...)
		b = append(b, err0.Error()...)
		b = append(b, " \033[0m"...)
	}

	b = append(b, "\r\n"...)
	if err0 == nil || err0 == sql.ErrNoRows {
		b = append(b, "\033[32m[OK]\033[0m"...)
	} else {
		b = append(b, "\033[31m[FAIL]\033[0m"...)
	}

	elapsed = elapsed / time.Millisecond
	b = append(b, " \033[33m["...)
	b = append(b, elapsed.String()...)
	b = append(b, "]\033[0m"...)

	b = append(b, " \033[35m"...)
	b = append(b, query...)
	b = append(b, "\033[0m"...)

	if len(args) > 0 {
		b = append(b, "  ["...)
		b = append(b, fmt.Sprintf("%v", args[0])...)
		for _, arg := range args[1:] {
			b = append(b, ", "...)
			b = append(b, fmt.Sprintf("%v", arg)...)
		}
		b = append(b, "]"...)
	}

	out := db.out
	if out == nil {
		out = defaultOutput
	}
	out(b)
}

func (db *loggedDB) Unwrap() DB {
	return db.DB
}

func (db *loggedDB) Exec(query string, args ...interface{}) (d sql.Result, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	d, err = db.DB.Exec(query, args...)
	return
}

func (db *loggedDB) ExecContext(ctx context.Context, query string, args ...interface{}) (d sql.Result, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	d, err = db.DB.ExecContext(ctx, query, args...)
	return
}

func (db *loggedDB) Prepare(query string) (d *sql.Stmt, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.Prepare(query)
	return
}

func (db *loggedDB) PrepareContext(ctx context.Context, query string) (d *sql.Stmt, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.PrepareContext(ctx, query)
	return
}

func (db *loggedDB) Query(query string, args ...interface{}) (d *sql.Rows, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	d, err = db.DB.Query(query, args...)
	return
}

func (db *loggedDB) QueryContext(ctx context.Context, query string, args ...interface{}) (d *sql.Rows, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	d, err = db.DB.QueryContext(ctx, query, args...)
	return
}

func (db *loggedDB) QueryRow(query string, args ...interface{}) (d *sql.Row) {
	defer func(t0 time.Time) { db.log(d.Err(), time.Since(t0), query, args...) }(time.Now())
	d = db.DB.QueryRow(query, args...)
	return
}

func (db *loggedDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) (d *sql.Row) {
	defer func(t0 time.Time) { db.log(d.Err(), time.Since(t0), query, args...) }(time.Now())
	d = db.DB.QueryRowContext(ctx, query, args...)
	return
}

func (db *loggedDB) Get(dest interface{}, query string, args ...interface{}) (err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	err = db.DB.Get(dest, query, args...)
	return
}

func (db *loggedDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	err = db.DB.GetContext(ctx, dest, query, args...)
	return
}

func (db *loggedDB) NamedExec(query string, arg interface{}) (d sql.Result, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.NamedExec(query, arg)
	return
}

func (db *loggedDB) NamedExecContext(ctx context.Context, query string, arg interface{}) (d sql.Result, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.NamedExecContext(ctx, query, arg)
	return
}

func (db *loggedDB) NamedQuery(query string, arg interface{}) (d *sqlx.Rows, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.NamedQuery(query, arg)
	return
}

func (db *loggedDB) PrepareNamed(query string) (d *sqlx.NamedStmt, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.PrepareNamed(query)
	return
}

func (db *loggedDB) PrepareNamedContext(ctx context.Context, query string) (d *sqlx.NamedStmt, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.PrepareNamedContext(ctx, query)
	return
}

func (db *loggedDB) Preparex(query string) (d *sqlx.Stmt, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.Preparex(query)
	return
}

func (db *loggedDB) PreparexContext(ctx context.Context, query string) (d *sqlx.Stmt, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query) }(time.Now())
	d, err = db.DB.PreparexContext(ctx, query)
	return
}

func (db *loggedDB) QueryRowx(query string, args ...interface{}) (d *sqlx.Row) {
	defer func(t0 time.Time) { db.log(d.Err(), time.Since(t0), query, args...) }(time.Now())
	d = db.DB.QueryRowx(query)
	return
}

func (db *loggedDB) QueryRowxContext(ctx context.Context, query string, args ...interface{}) (d *sqlx.Row) {
	defer func(t0 time.Time) { db.log(d.Err(), time.Since(t0), query, args...) }(time.Now())
	d = db.DB.QueryRowxContext(ctx, query, args...)
	return
}

func (db *loggedDB) Queryx(query string, args ...interface{}) (d *sqlx.Rows, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	d, err = db.DB.Queryx(query, args...)
	return
}

func (db *loggedDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (d *sqlx.Rows, err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	d, err = db.DB.QueryxContext(ctx, query, args...)
	return
}

func (db *loggedDB) Select(dest interface{}, query string, args ...interface{}) (err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	err = db.DB.Select(dest, query, args...)
	return
}

func (db *loggedDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	defer func(t0 time.Time) { db.log(err, time.Since(t0), query, args...) }(time.Now())
	err = db.DB.SelectContext(ctx, dest, query, args...)
	return
}

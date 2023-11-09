package sqlxcluster

import (
	"bytes"
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
)

var (
	colorRed    = []byte("\033[31m")
	colorGreen  = []byte("\033[32m")
	colorYellow = []byte("\033[33m")
	colorBlue   = []byte("\033[34m")
	colorPurple = []byte("\033[35m")
	colorWhite  = []byte("\033[37m")
	colorEnd    = []byte("\033[0m")

	_, _, _, _, _, _, _ = colorRed, colorGreen, colorYellow, colorBlue, colorPurple, colorWhite, colorEnd
)

var (
	_ logged = (*loggedDB)(nil)
	_ logged = (*loggedTx)(nil)
)

type logged interface {
	Logged() bool
	Colored() bool
	Output() func(b []byte) (int, error)
}

func defaultOut(b []byte) (int, error) {
	stdlog.Output(5, string(b))
	return len(b), nil
}

func writeColorBytes(b *bytes.Buffer, enableColor bool, color []byte, p []byte) *bytes.Buffer {
	if enableColor && len(color) > 0 {
		b.Write(color)
	}
	b.Write(p)
	if enableColor && len(color) > 0 {
		b.Write(colorEnd)
	}
	return b
}

func output(out func([]byte) (int, error), err0 error, enableColor bool, elapsed time.Duration, query string, args ...interface{}) {
	// 2009/01/23 01:23:23 /a/b/c/d.go:23: error
	// [OK] [200ms] select * from user

	var b = bytes.NewBuffer(nil)

	if err0 != nil && err0 != sql.ErrNoRows {
		writeColorBytes(b, enableColor, colorRed, []byte(err0.Error()))
	}

	b.WriteString("\r\n")
	if err0 == nil || err0 == sql.ErrNoRows {
		writeColorBytes(b, enableColor, colorGreen, []byte("[OK]"))
	} else {
		writeColorBytes(b, enableColor, colorRed, []byte("[FAIL]"))
	}

	elapsed = elapsed / time.Millisecond * time.Millisecond
	writeColorBytes(b, enableColor, colorYellow, []byte(" ["+elapsed.String()+"]"))
	b.WriteString(" ")
	writeColorBytes(b, enableColor, colorPurple, []byte(query))

	if len(args) > 0 {
		b.WriteString("  [")
		b.WriteString(fmt.Sprintf("%v", args[0]))
		for _, arg := range args[1:] {
			b.WriteString(", ")
			b.WriteString(fmt.Sprintf("%v", arg))
		}
		b.WriteString("]")
	}

	if out == nil {
		out = defaultOut
	}
	out(b.Bytes())
}

func NewLoggedDB(db DB, color bool, out func([]byte) (int, error)) DB {
	db = unwrapLoggedDB(db)
	return &loggedDB{DB: db, color: color, out: out}
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
	color bool
	out   func([]byte) (int, error)
}

func (db *loggedDB) Unwrap() DB {
	return db.DB
}

func (db *loggedDB) Logged() bool {
	return true
}

func (db *loggedDB) SetColor(color bool) {
	db.color = color
}

func (db *loggedDB) SetOutput(out func([]byte) (int, error)) {
	db.out = out
}

func (db *loggedDB) Colored() bool {
	return db.color
}

func (db *loggedDB) Output() func(b []byte) (int, error) {
	return db.out
}

func (db *loggedDB) Exec(query string, args ...interface{}) (d sql.Result, err error) {
	t0 := time.Now()
	d, err = db.DB.Exec(query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) ExecContext(ctx context.Context, query string, args ...interface{}) (d sql.Result, err error) {
	t0 := time.Now()
	d, err = db.DB.ExecContext(ctx, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) Prepare(query string) (d *sql.Stmt, err error) {
	t0 := time.Now()
	d, err = db.DB.Prepare(query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) PrepareContext(ctx context.Context, query string) (d *sql.Stmt, err error) {
	t0 := time.Now()
	d, err = db.DB.PrepareContext(ctx, query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) Query(query string, args ...interface{}) (d *sql.Rows, err error) {
	t0 := time.Now()
	d, err = db.DB.Query(query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) QueryContext(ctx context.Context, query string, args ...interface{}) (d *sql.Rows, err error) {
	t0 := time.Now()
	d, err = db.DB.QueryContext(ctx, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) QueryRow(query string, args ...interface{}) (d *sql.Row) {
	t0 := time.Now()
	d = db.DB.QueryRow(query, args...)
	output(db.out, d.Err(), db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) (d *sql.Row) {
	t0 := time.Now()
	d = db.DB.QueryRowContext(ctx, query, args...)
	output(db.out, d.Err(), db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) Get(dest interface{}, query string, args ...interface{}) (err error) {
	t0 := time.Now()
	err = db.DB.Get(dest, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	t0 := time.Now()
	err = db.DB.GetContext(ctx, dest, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) NamedExec(query string, arg interface{}) (d sql.Result, err error) {
	t0 := time.Now()
	d, err = db.DB.NamedExec(query, arg)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) NamedExecContext(ctx context.Context, query string, arg interface{}) (d sql.Result, err error) {
	t0 := time.Now()
	d, err = db.DB.NamedExecContext(ctx, query, arg)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) NamedQuery(query string, arg interface{}) (d *sqlx.Rows, err error) {
	t0 := time.Now()
	d, err = db.DB.NamedQuery(query, arg)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) PrepareNamed(query string) (d *sqlx.NamedStmt, err error) {
	t0 := time.Now()
	d, err = db.DB.PrepareNamed(query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) PrepareNamedContext(ctx context.Context, query string) (d *sqlx.NamedStmt, err error) {
	t0 := time.Now()
	d, err = db.DB.PrepareNamedContext(ctx, query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) Preparex(query string) (d *sqlx.Stmt, err error) {
	t0 := time.Now()
	d, err = db.DB.Preparex(query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) PreparexContext(ctx context.Context, query string) (d *sqlx.Stmt, err error) {
	t0 := time.Now()
	d, err = db.DB.PreparexContext(ctx, query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedDB) QueryRowx(query string, args ...interface{}) (d *sqlx.Row) {
	t0 := time.Now()
	d = db.DB.QueryRowx(query)
	output(db.out, d.Err(), db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) QueryRowxContext(ctx context.Context, query string, args ...interface{}) (d *sqlx.Row) {
	t0 := time.Now()
	d = db.DB.QueryRowxContext(ctx, query, args...)
	output(db.out, d.Err(), db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) Queryx(query string, args ...interface{}) (d *sqlx.Rows, err error) {
	t0 := time.Now()
	d, err = db.DB.Queryx(query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) QueryxContext(ctx context.Context, query string, args ...interface{}) (d *sqlx.Rows, err error) {
	t0 := time.Now()
	d, err = db.DB.QueryxContext(ctx, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) Select(dest interface{}, query string, args ...interface{}) (err error) {
	t0 := time.Now()
	err = db.DB.Select(dest, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	t0 := time.Now()
	err = db.DB.SelectContext(ctx, dest, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func NewLoggedTx(tx Tx, color bool, out func([]byte) (int, error)) Tx {
	return &loggedTx{Tx: tx, color: color, out: out}
}

type loggedTx struct {
	Tx
	color bool
	out   func([]byte) (int, error)
}

func (db *loggedTx) Unwrap() Tx {
	return db.Tx
}

func (db *loggedTx) Logged() bool {
	return true
}

func (db *loggedTx) SetColor(color bool) {
	db.color = color
}

func (db *loggedTx) SetOutput(out func([]byte) (int, error)) {
	db.out = out
}

func (db *loggedTx) Colored() bool {
	return db.color
}

func (db *loggedTx) Output() func(b []byte) (int, error) {
	return db.out
}

func (db *loggedTx) Exec(query string, args ...interface{}) (d sql.Result, err error) {
	t0 := time.Now()
	d, err = db.Tx.Exec(query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) ExecContext(ctx context.Context, query string, args ...interface{}) (d sql.Result, err error) {
	t0 := time.Now()
	d, err = db.Tx.ExecContext(ctx, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) Prepare(query string) (d *sql.Stmt, err error) {
	t0 := time.Now()
	d, err = db.Tx.Prepare(query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) PrepareContext(ctx context.Context, query string) (d *sql.Stmt, err error) {
	t0 := time.Now()
	d, err = db.Tx.PrepareContext(ctx, query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) Query(query string, args ...interface{}) (d *sql.Rows, err error) {
	t0 := time.Now()
	d, err = db.Tx.Query(query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) QueryContext(ctx context.Context, query string, args ...interface{}) (d *sql.Rows, err error) {
	t0 := time.Now()
	d, err = db.Tx.QueryContext(ctx, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) QueryRow(query string, args ...interface{}) (d *sql.Row) {
	t0 := time.Now()
	d = db.Tx.QueryRow(query, args...)
	output(db.out, d.Err(), db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) (d *sql.Row) {
	t0 := time.Now()
	d = db.Tx.QueryRowContext(ctx, query, args...)
	output(db.out, d.Err(), db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) Get(dest interface{}, query string, args ...interface{}) (err error) {
	t0 := time.Now()
	err = db.Tx.Get(dest, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	t0 := time.Now()
	err = db.Tx.GetContext(ctx, dest, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) NamedExec(query string, arg interface{}) (d sql.Result, err error) {
	t0 := time.Now()
	d, err = db.Tx.NamedExec(query, arg)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) NamedExecContext(ctx context.Context, query string, arg interface{}) (d sql.Result, err error) {
	t0 := time.Now()
	d, err = db.Tx.NamedExecContext(ctx, query, arg)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) NamedQuery(query string, arg interface{}) (d *sqlx.Rows, err error) {
	t0 := time.Now()
	d, err = db.Tx.NamedQuery(query, arg)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) PrepareNamed(query string) (d *sqlx.NamedStmt, err error) {
	t0 := time.Now()
	d, err = db.Tx.PrepareNamed(query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) PrepareNamedContext(ctx context.Context, query string) (d *sqlx.NamedStmt, err error) {
	t0 := time.Now()
	d, err = db.Tx.PrepareNamedContext(ctx, query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) Preparex(query string) (d *sqlx.Stmt, err error) {
	t0 := time.Now()
	d, err = db.Tx.Preparex(query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) PreparexContext(ctx context.Context, query string) (d *sqlx.Stmt, err error) {
	t0 := time.Now()
	d, err = db.Tx.PreparexContext(ctx, query)
	output(db.out, err, db.color, time.Since(t0), query)
	return
}

func (db *loggedTx) QueryRowx(query string, args ...interface{}) (d *sqlx.Row) {
	t0 := time.Now()
	d = db.Tx.QueryRowx(query)
	output(db.out, d.Err(), db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) QueryRowxContext(ctx context.Context, query string, args ...interface{}) (d *sqlx.Row) {
	t0 := time.Now()
	d = db.Tx.QueryRowxContext(ctx, query, args...)
	output(db.out, d.Err(), db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) Queryx(query string, args ...interface{}) (d *sqlx.Rows, err error) {
	t0 := time.Now()
	d, err = db.Tx.Queryx(query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) QueryxContext(ctx context.Context, query string, args ...interface{}) (d *sqlx.Rows, err error) {
	t0 := time.Now()
	d, err = db.Tx.QueryxContext(ctx, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) Select(dest interface{}, query string, args ...interface{}) (err error) {
	t0 := time.Now()
	err = db.Tx.Select(dest, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

func (db *loggedTx) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) (err error) {
	t0 := time.Now()
	err = db.Tx.SelectContext(ctx, dest, query, args...)
	output(db.out, err, db.color, time.Since(t0), query, args...)
	return
}

package sqlxcluster

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type itx interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type itxx interface {
	Beginx() (*sqlx.Tx, error)
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
}

func Begin(db DB) (tx Tx, err error) {
	return BeginTx(db, context.Background(), nil)
}

func BeginTx(db DB, ctx context.Context, opts *sql.TxOptions) (tx Tx, err error) {
	tx, err = db.BeginTxx(ctx, opts)
	if err != nil {
		return tx, err
	}
	if ldb, ok := db.(logged); ok && ldb.Logged() {
		tx = NewLoggedTx(tx, ldb.Colored(), ldb.Output())
	}
	return tx, err
}

func RunInTx(db itx, fn func(tx *sql.Tx) error) (err error) {
	return RunInTxWithOpts(db, context.Background(), nil, fn)
}

func RunInTxWithOpts(db itx, ctx context.Context, opts *sql.TxOptions, fn func(tx *sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	rolledBack := false
	defer func() {
		if r := recover(); r != nil {
			if !rolledBack {
				_ = tx.Rollback()
			}
			panic(r)
		}
	}()
	if err = fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			err = fmt.Errorf("%v (rollback error: %v)", err, rbErr)
		}
		rolledBack = true
		return err
	}
	return tx.Commit()
}

func RunInTxx(db itxx, fn func(tx *sqlx.Tx) error) (err error) {
	return RunInTxxWithOpts(db, context.Background(), nil, fn)
}

func RunInTxxWithOpts(db itxx, ctx context.Context, opts *sql.TxOptions, fn func(tx *sqlx.Tx) error) (err error) {
	tx, err := db.BeginTxx(ctx, opts)
	if err != nil {
		return err
	}
	rolledBack := false
	defer func() {
		if r := recover(); r != nil {
			if !rolledBack {
				_ = tx.Rollback()
			}
			panic(r)
		}
	}()
	if err = fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			err = fmt.Errorf("%v (rollback error: %v)", err, rbErr)
		}
		rolledBack = true
		return err
	}
	return tx.Commit()
}

func RunInClusterTx(db DB, fn func(tx Tx) error) (err error) {
	return RunInClusterTxWithOpts(db, context.Background(), nil, fn)
}

func RunInClusterTxWithOpts(db DB, ctx context.Context, opts *sql.TxOptions, fn func(tx Tx) error) (err error) {
	tx, err := BeginTx(db, ctx, opts)
	if err != nil {
		return err
	}
	rolledBack := false
	defer func() {
		if r := recover(); r != nil {
			if !rolledBack {
				_ = tx.Rollback()
			}
			panic(r)
		}
	}()
	if err = fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			err = fmt.Errorf("%v (rollback error: %v)", err, rbErr)
		}
		rolledBack = true
		return err
	}
	return tx.Commit()
}

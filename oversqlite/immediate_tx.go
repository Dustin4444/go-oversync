package oversqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
)

// immediateTx owns one dedicated SQLite connection between BEGIN IMMEDIATE
// and COMMIT/ROLLBACK. database/sql's deferred BeginTx does not reserve the
// writer before the final no-data-loss gate.
type immediateTx struct {
	conn *sql.Conn
	ctx  context.Context
	done bool
}

func beginImmediateTx(ctx context.Context, db *sql.DB) (*immediateTx, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to reserve SQLite connection for immediate transaction: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to begin immediate SQLite transaction: %w", err)
	}
	return &immediateTx{conn: conn, ctx: ctx}, nil
}

func (tx *immediateTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.conn.ExecContext(ctx, query, args...)
}

func (tx *immediateTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.conn.QueryContext(ctx, query, args...)
}

func (tx *immediateTx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.conn.QueryRowContext(ctx, query, args...)
}

func (tx *immediateTx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.conn.QueryContext(tx.ctx, query, args...)
}

func (tx *immediateTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return tx.conn.PrepareContext(ctx, query)
}

func (tx *immediateTx) Commit(ctx context.Context) error {
	if tx == nil || tx.done {
		return fmt.Errorf("immediate SQLite transaction is not active")
	}
	if _, err := tx.conn.ExecContext(ctx, `COMMIT`); err != nil {
		return err
	}
	tx.done = true
	_ = tx.conn.Close()
	return nil
}

func (tx *immediateTx) Rollback() error {
	if tx == nil || tx.done {
		return nil
	}
	rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, rollbackErr := tx.conn.ExecContext(rollbackCtx, `ROLLBACK`)
	tx.done = true
	if rollbackErr != nil {
		discardErr := tx.conn.Raw(func(any) error { return driver.ErrBadConn })
		if discardErr != nil && !errors.Is(discardErr, driver.ErrBadConn) && !errors.Is(discardErr, sql.ErrConnDone) {
			return errors.Join(rollbackErr, fmt.Errorf("failed to discard SQLite connection after rollback failure: %w", discardErr))
		}
		return rollbackErr
	}
	return tx.conn.Close()
}

func (tx *immediateTx) rollbackOnReturn(resultErr *error, operation string) {
	rollbackErr := tx.Rollback()
	if rollbackErr == nil {
		return
	}
	cleanupErr := fmt.Errorf("failed to roll back %s: %w", operation, rollbackErr)
	if *resultErr == nil {
		*resultErr = cleanupErr
		return
	}
	*resultErr = errors.Join(*resultErr, cleanupErr)
}

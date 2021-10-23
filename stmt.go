// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"
)

// cancellableMysqlStfmt is a prepared statement.
// A cancellableMysqlStfmt is safe for concurrent use by multiple goroutines.
type cancellableMysqlStfmt struct {
	stmt         driver.Stmt
	killerPool   *sql.DB
	connectionID string
	kto          time.Duration
}

// Unleak will release the reference to the killerPool
// in order to prevent a memory leak.
func (s *cancellableMysqlStfmt) Unleak() {
	s.killerPool = nil
	s.connectionID = ""
	s.kto = 0
}

// Close closes the statement.
func (s *cancellableMysqlStfmt) Close() error {
	err := s.stmt.Close()
	if err != nil {
		return err
	}
	s.Unleak() // Should this be called in a defer to guarantee it gets called?
	return nil
}

func (s *cancellableMysqlStfmt) NumInput() int {
	return s.stmt.NumInput()
}

// Exec executes a prepared statement with the given arguments and
// returns a Result summarizing the effect of the statement.
func (s *cancellableMysqlStfmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.stmt.Exec(args)
}

// ExecContext executes a prepared statement with the given arguments and
// returns a Result summarizing the effect of the statement.
func (s *cancellableMysqlStfmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	var stmtExecContext = s.stmt.(driver.StmtExecContext)

	// Create a context that is used to cancel ExecContext()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	outChan := make(chan sql.Result)
	errChan := make(chan error)
	returnedChan := make(chan struct{}) // Used to indicate that this function has returned

	defer func() {
		returnedChan <- struct{}{}
	}()

	go func() {
		select {
		case <-ctx.Done():
			// context has been canceled
			kill(s.killerPool, s.connectionID, s.kto)
			errChan <- ctx.Err()
		case <-returnedChan:
		}
	}()

	go func() {
		res, err := stmtExecContext.ExecContext(cancelCtx, args)
		if err != nil {
			errChan <- err
			return
		}
		outChan <- res
	}()

	select {
	case err := <-errChan:
		return nil, err
	case out := <-outChan:
		return out, nil
	}
}

// Query executes a prepared query statement with the given arguments
// and returns the query results as a *cancellableMysqlRows.
func (s *cancellableMysqlStfmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.stmt.Query(args)
}

// QueryContext executes a prepared query statement with the given arguments
// and returns the query results as a *cancellableMysqlRows.
func (s *cancellableMysqlStfmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var stmtQueryContext = s.stmt.(driver.StmtQueryContext)

	// We can't use the same approach used in ExecContext because defer cancelFunc()
	// cancels rows.Scan.
	defer func() {
		if ctx.Err() != nil {
			kill(s.killerPool, s.connectionID, s.kto)
		}
	}()

	rows, err := stmtQueryContext.QueryContext(ctx, args)
	return &cancellableMysqlRows{ctx: ctx, rows: rows, killerPool: s.killerPool, connectionID: s.connectionID}, err
}

func (s *cancellableMysqlStfmt) ColumnConverter(idx int) driver.ValueConverter {
	var columnConverter = s.stmt.(driver.ColumnConverter)
	return columnConverter.ColumnConverter(idx)
}

func (s *cancellableMysqlStfmt) CheckNamedValue(nv *driver.NamedValue) (err error) {
	var namedValueChecker = s.stmt.(driver.NamedValueChecker)
	return namedValueChecker.CheckNamedValue(nv)
}

// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"time"
)

type cancellableMysqlConn struct {
	conn         driver.Conn
	killerPool   *sql.DB
	connectionID string
	kto          time.Duration
}

func new_cancellableMySQLConn(conn driver.Conn, db *sql.DB, ConnectionID string, kto time.Duration) *cancellableMysqlConn{
	_ = mysql.SetLogger(log.New(ioutil.Discard, "", 0))
	log.Printf("New connection %s created!", ConnectionID)
	return &cancellableMysqlConn{conn, db, ConnectionID, kto}
}

func (c *cancellableMysqlConn) Unleak() {
	c.killerPool = nil
	c.connectionID = ""
}
func (c *cancellableMysqlConn) Ping(ctx context.Context) error {
	var connPinger = c.conn.(driver.Pinger)

	// You can not cancel a Ping.
	// See: https://github.com/rocketlaunchr/mysql-go/issues/3
	return connPinger.Ping(ctx)
}

func (c *cancellableMysqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	var execer = c.conn.(driver.Execer)
	return execer.Exec(query, args)
}

func (c *cancellableMysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var execerContext = c.conn.(driver.ExecerContext)

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
			kill(c.killerPool, c.connectionID, c.kto)
			errChan <- ctx.Err()
		case <-returnedChan:
		}
	}()

	go func() {
		res, err := execerContext.ExecContext(cancelCtx, query, args)
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

func (c *cancellableMysqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	var queryer = c.conn.(driver.Queryer)
	return queryer.Query(query, args)
}

func (c *cancellableMysqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var queryerContext = c.conn.(driver.QueryerContext)

	// We can't use the same approach used in ExecContext because defer cancelFunc()
	// cancels rows.Scan.
	defer func() {
		if ctx.Err() != nil {
			kill(c.killerPool, c.connectionID, c.kto)

		}
	}()

	rows, err := queryerContext.QueryContext(ctx, query, args)
	return &cancellableMysqlRows{ctx: ctx, rows: rows, killerPool: c.killerPool, connectionID: c.connectionID}, err
}

func (c *cancellableMysqlConn) Prepare(query string) (driver.Stmt, error) {
	return c.conn.Prepare(query)
}

func (c *cancellableMysqlConn) Close() error {
	err := c.conn.Close()
	if err != nil {
		return err
	}
	c.Unleak() // Should this be called in a defer to guarantee it gets called?
	return nil
}

func (c *cancellableMysqlConn) Begin() (driver.Tx, error) {
	return c.conn.Begin()
}

func (c *cancellableMysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var connPrepareContext = c.conn.(driver.ConnPrepareContext)

	// You can not cancel a Prepare.
	// See: https://github.com/rocketlaunchr/mysql-go/issues/3
	stmt, err := connPrepareContext.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &cancellableMysqlStfmt{stmt, c.killerPool, c.connectionID, c.kto}, nil
}

func (c *cancellableMysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var connBeginTx = c.conn.(driver.ConnBeginTx)
	return connBeginTx.BeginTx(ctx, opts)
}

func (c *cancellableMysqlConn) ResetSession(ctx context.Context) error {
	var sessionResetter = c.conn.(driver.SessionResetter)
	return sessionResetter.ResetSession(ctx)
}

/*
func (c *cancellableMysqlConn) IsValid() bool {
	var validator = c.conn.(driver.Validator)
	return validator.IsValid()
}
*/

func (c *cancellableMysqlConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	var namedValueChecker = c.conn.(driver.NamedValueChecker)
	return namedValueChecker.CheckNamedValue(nv)
}
func Bench(){

}

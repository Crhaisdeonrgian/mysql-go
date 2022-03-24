// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"
	"time"
)

const (
	cancellableDriverName = "mysqlc"
	defaultKillPoolSize   = 1
	defaultKillTimeout    = 5 * time.Second
)

type CancellableMySQLDriver struct{}

var originalDriver = mysql.MySQLDriver{}

var CancelModeUsage bool

func init() {
	sql.Register(cancellableDriverName, &CancellableMySQLDriver{})
}

// Open new Connection.
// See https://github.com/go-sql-driver/mysql#dsn-data-source-name for how
// the DSN string is formatted
func (d CancellableMySQLDriver) Open(dsn string) (driver.Conn, error) {
	var c, err = d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}

	return c.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.
func (d CancellableMySQLDriver) OpenConnector(dsn string) (driver.Connector, error) {
	var cfg, err = ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	var connector driver.Connector
	if connector, err = originalDriver.OpenConnector(dsn); err != nil {
		return nil, err
	}

	var killConnector driver.Connector
	if killConnector, err = originalDriver.OpenConnector(dsn); err != nil {
		return nil, err
	}

	var killPool = sql.OpenDB(killConnector)
	killPool.SetMaxOpenConns(cfg.killPoolSize)
	return &cancellableConnector{
		connector:   connector,
		killPool:    killPool,
		killTimeout: cfg.killTimeout,
	}, nil
}

type cancellableConnector struct {
	connector   driver.Connector
	killPool    *sql.DB
	killTimeout time.Duration
}

func (c *cancellableConnector) Connect(ctx context.Context) (driver.Conn, error) {
	var conn, err = c.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	// Determine the connection's connection_id
	var connectionID string
	if(CancelModeUsage){
		if connectionID, err = determineConnectionId(ctx, conn); err != nil {
			conn.Close()
			return nil, err
		}
	}

	if c.killPool == nil {
		return new_cancellableMySQLConn(conn, c.killPool, connectionID, c.killTimeout), nil
	}
	return new_cancellableMySQLConn(conn, c.killPool, connectionID, c.killTimeout), nil
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.

// Driver implements driver.Connector interface.
// Driver returns &CancellableMySQLDriver{}.
func (c *cancellableConnector) Driver() driver.Driver {
	return &CancellableMySQLDriver{}
}

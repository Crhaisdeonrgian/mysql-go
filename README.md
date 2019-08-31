# Canceling MySQL in Go [![GoDoc](http://godoc.org/github.com/rocketlaunchr/mysql-go?status.svg)](http://godoc.org/github.com/rocketlaunchr/mysql-go) [![Go Report Card](https://goreportcard.com/badge/github.com/rocketlaunchr/mysql-go)](https://goreportcard.com/report/github.com/rocketlaunchr/mysql-go)

This package will properly implement context cancelation for MySQL. Without this package, context cancelation does not actually cancel a MySQL query.

See [Article](https://medium.com/@rocketlaunchr.cloud/canceling-mysql-in-go-827ed8f83b30) for details of the behind-the-scenes magic.

The API is designed to resemble the standard library. It is fully compatible with the [dbq](https://github.com/rocketlaunchr/dbq) package which allows for zero boilerplate database operations in Go.

## Dependencies

-   [Go MySQL Driver](https://github.com/go-sql-driver/mysql)

## Installation

```
go get -u github.com/rocketlaunchr/mysql-go
```

## QuickStart

```go

import (
   stdSql "database/sql"
   sql "github.com/rocketlaunchr/mysql-go"
)

p, _ := stdSql.Open("mysql", "user:password@/dbname")
kP, _ := stdSql.Open("mysql", "user:password@/dbname") // KillerPool
kP.SetMaxOpenConns(1)

pool := &sql.DB{p, kP}

```

## Read Query

```go

// Obtain an exclusive connection
conn, err := pool.Conn(ctx)
defer conn.Close() // Return the connection back to the pool

// Perform your read operation.
rows, err := conn.QueryContext(ctx, stmt)
if err != nil {
   return err
}

```

## Write Query

```go

// Obtain an exclusive connection
conn, err := pool.Conn(ctx)
defer conn.Close() // Return the connection back to the pool

// Perform the write operation
tx, err := conn.BeginTx(ctx, nil)

_, err = tx.ExecContext(ctx, stmt)
if err != nil {
   return tx.Rollback()
}

tx.Commit()
```

## Cancel Query

Cancel the context. This will send a `KILL` signal to MySQL automatically.

It is highly recommended you set a KillerPool when you instantiate the `DB` object.

The KillerPool is used to call the `KILL` signal.

## Reverse Proxy Support

Checkout the `proxy-protection` branch if your database is behind a reverse proxy in order to better guarantee that you are killing the correct query.

#

### Legal Information

The license is a modified MIT license. Refer to `LICENSE` file for more details.

**© 2018-19 PJ Engineering and Business Solutions Pty. Ltd.**

### Final Notes

Feel free to enhance features by issuing pull-requests.

**Star** the project to show your appreciation.

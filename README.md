# Canceling MySQL in Go

A MySQL-Driver for Go's [database/sql](https://golang.org/pkg/database/sql/) package which properly implement context cancelation for MySQL.

See [Article](https://medium.com/@rocketlaunchr.cloud/canceling-mysql-in-go-827ed8f83b30) for details of the behind-the-scenes magic.

---------------------------------------
* [Features](#features)
* [Requirements](#requirements)
* [Installation](#installation)
* [Usage](#usage)
    * [DSN (Data Source Name)](#dsn-data-source-name)
        * [Parameters](#parameters)
    * [Cancel Query](#cancel-query)
* [License](#license)
* [Final Notes](#final-notes)

---------------------------------------

## Features
* Wrapper over standard [Go MySQL Driver](https://github.com/go-sql-driver/mysql)
* Automatic killing queries on context cancellation 
* Automatic connection pooling (by [database/sql](https://golang.org/pkg/database/sql/) package)


## Requirements
The same as for imported version of [Go MySQL Driver](https://github.com/go-sql-driver/mysql)

---------------------------------------

## Installation
Simple install the package to your [$GOPATH](https://github.com/golang/go/wiki/GOPATH "GOPATH") with the [go tool](https://golang.org/cmd/go/ "go command") from shell:
```bash
$ go get -u github.com/dati-mipt/mysql-go
```
Make sure [Git is installed](https://git-scm.com/downloads) on your machine and in your system's `PATH`.

## Usage
This is implementation of Go's `database/sql/driver` interface. You only need to import the driver and can use the full [`database/sql`](https://golang.org/pkg/database/sql/) API then.

Use `mysqlc` as `driverName` and a valid [DSN](#dsn-data-source-name)  as `dataSourceName`:

```go
import (
	"database/sql"
	"time"

	_ "github.com/dati-mipt/mysql-go"
)

// ...

db, err := sql.Open("mysqlc", "user:password@/dbname")
if err != nil {
	panic(err)
}
// See "Important settings" section.
db.SetConnMaxLifetime(time.Minute * 3)
db.SetMaxOpenConns(10)
db.SetMaxIdleConns(10)
```

[Examples are available in Wiki of standard mysql driver](https://github.com/go-sql-driver/mysql/wiki/Examples "Go-MySQL-Driver Examples").

### DSN (Data Source Name)

The Data Source Name has a common format, like e.g. [PEAR DB](http://pear.php.net/manual/en/package.database.db.intro-dsn.php) uses it, but without type-prefix (optional parts marked by squared brackets):
```
[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
```

#### Parameters
*Parameters are case-sensitive!*

In addition to [Go MySQL Driver](https://github.com/go-sql-driver/mysql) parameters, this wrapper introduces some new params:

##### `killPoolSize`
```
Type:          decimal number
Default:       1
```

Size of connection pool for killing queries.

##### `killTimeout`

```
Type:           duration
Default:        5s
```

Timeout of kill operation.

### Cancel Query

Cancel the context. This will send a `KILL` signal to MySQL automatically.

## License

The license is a modified MIT license. Refer to `LICENSE` file for more details.

**© 2018-19 PJ Engineering and Business Solutions Pty. Ltd.**

## Final Notes

Feel free to enhance features by issuing pull-requests..

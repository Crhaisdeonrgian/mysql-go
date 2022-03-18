// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

func determineConnectionId(ctx context.Context, conn driver.Conn) (string, error) {
	var querierCtx = conn.(driver.QueryerContext)

	var rows, err = querierCtx.QueryContext(ctx, "SELECT CONNECTION_ID()", []driver.NamedValue{})
	if err != nil {
		return "", err
	}

	defer rows.Close()

	var queryResult = make([]driver.Value, len(rows.Columns()))
	if err = rows.Next(queryResult); err != nil {
		return "", err
	}

	if len(queryResult) != 1 {
		return "", fmt.Errorf("sql: expected only one connection id in query results, not %d", len(queryResult))
	}

	var value = queryResult[0]
	var connectionID = string(value.([]uint8))

	return connectionID, nil
}

// kill is used to kill a running query.
// It is advised that db be another pool that the
// connection was NOT derived from.
func kill(db *sql.DB, connectionID string, kto time.Duration) error {

	if connectionID == "" {
		return nil
	}

	var qry = fmt.Sprintf("KILL QUERY %s", connectionID)

	if kto == 0 {
		_, err := db.Exec(qry)
		fmt.Printf("Connection %s killed\n", connectionID)
		if err != nil {
			return err
		}
	} else {
		ctx, cancelFunc := context.WithTimeout(context.Background(), kto)
		defer cancelFunc()
		_, err := db.ExecContext(ctx, qry)
		if err != nil {
			return err
		}
	}

	return nil
}

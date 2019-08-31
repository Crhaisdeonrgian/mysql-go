// Copyright 2018-19 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package sql

// kill is used to kill a running query.
// It is advised that db be another pool that the
// connection was NOT derived from.
func kill(db StdSQLDB, connectionID string) error {

	if connectionID == "" {
		return nil
	}

	_, err := db.Exec("KILL QUERY ?", connectionID)
	if err != nil {
		return err
	}

	return nil
}

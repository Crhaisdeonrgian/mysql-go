package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"time"
)

type cancellableMysqlRows struct {
	ctx          context.Context
	rows         driver.Rows
	killerPool   *sql.DB
	connectionID string
	kto          time.Duration
}

func (rs *cancellableMysqlRows) Columns() []string {
	var cols = rs.rows.Columns()
	if rs.ctx.Err() != nil {
		kill(rs.killerPool, rs.connectionID, rs.kto)
	}
	return cols
}

// Unleak will release the reference to the killerPool
// in order to prevent a memory leak.
func (rs *cancellableMysqlRows) Unleak() {
	rs.killerPool = nil
	rs.connectionID = ""
	rs.kto = 0
}

func (rs *cancellableMysqlRows) Close() error {
	err := rs.rows.Close()
	if rs.ctx.Err() != nil {
		kill(rs.killerPool, rs.connectionID, rs.kto)
	}
	rs.Unleak()
	return err
}

func (rs *cancellableMysqlRows) Next(dest []driver.Value) error {
	return rs.rows.Next(dest)
}

func (rs *cancellableMysqlRows) HasNextResultSet() bool {
	var rowsNextResultSet, ok = rs.rows.(driver.RowsNextResultSet)
	if !ok {
		return false
	}

	return rowsNextResultSet.HasNextResultSet()
}

func (rs *cancellableMysqlRows) NextResultSet() error {
	var rowsNextResultSet = rs.rows.(driver.RowsNextResultSet)
	return rowsNextResultSet.NextResultSet()
}

func (rs *cancellableMysqlRows) ColumnTypeScanType(index int) reflect.Type {
	var rowsColumnTypeScanType = rs.rows.(driver.RowsColumnTypeScanType)
	return rowsColumnTypeScanType.ColumnTypeScanType(index)
}

func (rs *cancellableMysqlRows) ColumnTypeDatabaseTypeName(index int) string {
	var rowsColumnTypeDatabaseTypeName = rs.rows.(driver.RowsColumnTypeDatabaseTypeName)
	return rowsColumnTypeDatabaseTypeName.ColumnTypeDatabaseTypeName(index)
}

func (rs *cancellableMysqlRows) ColumnTypeNullable(index int) (bool, bool) {
	var rowsColumnTypeNullable = rs.rows.(driver.RowsColumnTypeNullable)
	return rowsColumnTypeNullable.ColumnTypeNullable(index)
}

func (rs *cancellableMysqlRows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	var rowsColumnTypePrecisionScale = rs.rows.(driver.RowsColumnTypePrecisionScale)
	return rowsColumnTypePrecisionScale.ColumnTypePrecisionScale(index)

}

package gorp

import (
	"reflect"
)

type CockroachDialect struct {
	PostgresDialect
}

func (d CockroachDialect) ToSqlType(val reflect.Type, maxsize int, isAutoIncr bool) string {
	switch val.Kind() {
	case reflect.Ptr:
		return d.ToSqlType(val.Elem(), maxsize, isAutoIncr)
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		if isAutoIncr {
			return "bigint"
		}
		return "bigint"
	case reflect.Int64, reflect.Uint64:
		if isAutoIncr {
			return "bigint"
		}
		return "bigint"
	case reflect.Float64:
		return "float"
	case reflect.Float32:
		return "float"
	case reflect.Slice:
		if val.Elem().Kind() == reflect.Uint8 {
			return "bytes"
		}
	}

	switch val.Name() {
	case "NullInt64":
		return "bigint"
	case "NullFloat64":
		return "float"
	case "NullBool":
		return "boolean"
	case "Time", "NullTime":
		return "timestamp"
	}

	return "text"
}

func (d CockroachDialect) AutoIncrBindValue() string {
	return "experimental_unique_int()"
}

func (d CockroachDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return ""
}

func (d CockroachDialect) InsertAutoIncrToTarget(exec SqlExecutor, insertSql string, target interface{}, params ...interface{}) error {
	rows, err := exec.query(insertSql, params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		//cockroach does not seems to have facility like mysql's last_insert_id(). so ignore this error.
		return nil
	}
	if err := rows.Scan(target); err != nil {
		return err
	}
	if rows.Next() {
		return fmt.Errorf("more than two serial value returned for insert: %s", insertSql)
	}
	return rows.Err()
}

func (d CockroachDialect) IfSchemaNotExists(command, schema string) string {
	return "create database if not exists"
}

func NewCockroachDialect() CockroachDialect {
	return CockroachDialect{PostgresDialect{}}
}

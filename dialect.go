package goose

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/kshvakov/clickhouse"
)

// SQLDialect abstracts the details of specific SQL dialects
// for goose's few SQL specific statements
type SQLDialect interface {
	createVersionTableSQL(tableName string) string // sql string to create the goose_db_version table
	insertVersionSQL(tableName string) string      // sql string to insert the initial version table row
	dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error)
}

var dialect SQLDialect = &PostgresDialect{}

// GetDialect gets the SQLDialect
func GetDialect() SQLDialect {
	return dialect
}

// SetDialect sets the SQLDialect
func SetDialect(d string) error {
	switch d {
	case "postgres":
		dialect = &PostgresDialect{}
	case "mysql":
		dialect = &MySQLDialect{}
	case "sqlite3":
		dialect = &Sqlite3Dialect{}
	case "redshift":
		dialect = &RedshiftDialect{}
	case "tidb":
		dialect = &TiDBDialect{}
	case "clickhouse":
		dialect = &ClickhouseDialect{}
	default:
		return fmt.Errorf("%q: unknown dialect", d)
	}

	return nil
}

////////////////////////////
// Postgres
////////////////////////////

// PostgresDialect struct.
type PostgresDialect struct{}

func (pg PostgresDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s_db_version (
            	id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, tableName)
}

func (pg PostgresDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s_db_version (version_id, is_applied) VALUES ($1, $2);", tableName)
}

func (pg PostgresDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	sqlQuery := fmt.Sprintf("SELECT version_id, is_applied from %s_db_version ORDER BY id DESC", tableName)
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	return rows, err
}

////////////////////////////
// MySQL
////////////////////////////

// MySQLDialect struct.
type MySQLDialect struct{}

func (m MySQLDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s_db_version (
                id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, tableName)
}

func (m MySQLDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s_db_version (version_id, is_applied) VALUES (?, ?);", tableName)
}

func (m MySQLDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	sqlQuery := fmt.Sprintf("SELECT version_id, is_applied from %s_db_version ORDER BY id DESC", tableName)
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	return rows, err
}

////////////////////////////
// sqlite3
////////////////////////////

// Sqlite3Dialect struct.
type Sqlite3Dialect struct{}

func (m Sqlite3Dialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s_db_version (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                is_applied INTEGER NOT NULL,
                tstamp TIMESTAMP DEFAULT (datetime('now'))
            );`, tableName)
}

func (m Sqlite3Dialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s_db_version (version_id, is_applied) VALUES (?, ?);", tableName)
}

func (m Sqlite3Dialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	sqlQuery := fmt.Sprintf("SELECT version_id, is_applied from %s_db_version ORDER BY id DESC", tableName)
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	return rows, err
}

////////////////////////////
// Redshift
////////////////////////////

// RedshiftDialect struct.
type RedshiftDialect struct{}

func (rs RedshiftDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s_db_version (
            	id integer NOT NULL identity(1, 1),
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default sysdate,
                PRIMARY KEY(id)
            );`, tableName)
}

func (rs RedshiftDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s_db_version (version_id, is_applied) VALUES ($1, $2);", tableName)
}

func (rs RedshiftDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	sqlQuery := fmt.Sprintf("SELECT version_id, is_applied from %s_db_version ORDER BY id DESC", tableName)
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	return rows, err
}

////////////////////////////
// TiDB
////////////////////////////

// TiDBDialect struct.
type TiDBDialect struct{}

func (m TiDBDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s_db_version (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, tableName)
}

func (m TiDBDialect) insertVersionSQL(tableName string) string {
	return fmt.Sprintf("INSERT INTO %s_db_version (version_id, is_applied) VALUES (?, ?);", tableName)
}

func (m TiDBDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	sqlQuery := fmt.Sprintf("SELECT version_id, is_applied from %s_db_version ORDER BY id DESC", tableName)
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	return rows, err
}

////////////////////////////
// Clickhouse
////////////////////////////

// ClickhouseDialect struct.
type ClickhouseDialect struct{}

func (ch ClickhouseDialect) createVersionTableSQL(tableName string) string {
	return fmt.Sprintf(`CREATE TABLE %s_db_version (
				version_id Int64,
				is_applied UInt8 DEFAULT 0,
				tstamp DateTime DEFAULT now(),
				date Date DEFAULT toDate(tstamp)
			) engine=MergeTree(date, (version_id, tstamp), 8192)`, tableName)
}

func (ch ClickhouseDialect) insertVersionSQL(tableName string) string {
	time.Sleep(time.Second * 1)
	return fmt.Sprintf("INSERT INTO %s_db_version (version_id, is_applied) VALUES ($1, $2);", tableName)
}

func (ch ClickhouseDialect) dbVersionQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	sqlQuery := fmt.Sprintf("SELECT version_id, is_applied from %s_db_version ORDER BY tstamp DESC", tableName)
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	return rows, err
}

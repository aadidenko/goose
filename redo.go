package goose

import (
	"database/sql"
)

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(db *sql.DB, dir string, tableName string) error {
	currentVersion, err := GetDBVersion(db, tableName)
	if err != nil {
		return err
	}

	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	current, err := migrations.Current(currentVersion)
	if err != nil {
		return err
	}

	if err := current.Down(db, tableName); err != nil {
		return err
	}

	if err := current.Up(db, tableName); err != nil {
		return err
	}

	return nil
}

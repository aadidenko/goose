package goose

import (
	"database/sql"
	"fmt"
	"log"
)

// Down rolls back a single migration from the current version.
func Down(db *sql.DB, dir string, tableName string) error {
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
		return fmt.Errorf("no migration %v", currentVersion)
	}

	return current.Down(db, tableName)
}

// DownTo rolls back migrations to a specific version.
func DownTo(db *sql.DB, dir string, tableName string, version int64) error {
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	for {
		currentVersion, err := GetDBVersion(db, tableName)
		if err != nil {
			return err
		}

		current, err := migrations.Current(currentVersion)
		if err != nil {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
			return nil
		}

		if current.Version <= version {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
			return nil
		}

		if err = current.Down(db, tableName); err != nil {
			return err
		}
	}
}

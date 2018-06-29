package goose

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"time"
)

// Status prints the status of all migrations.
func Status(db *sql.DB, dir string, tableName string) error {
	// collect all migrations
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	// must ensure that the version table exists if we're running on a pristine DB
	if _, err := EnsureDBVersion(db, tableName); err != nil {
		return err
	}

	log.Println("    Applied At                  Migration")
	log.Println("    =======================================")
	for _, migration := range migrations {
		printMigrationStatus(db, tableName, migration.Version, filepath.Base(migration.Source))
	}

	return nil
}

func printMigrationStatus(db *sql.DB, tableName string, version int64, script string) {
	var row MigrationRecord
	q := fmt.Sprintf("SELECT tstamp, is_applied FROM %s_db_version WHERE version_id=%d ORDER BY tstamp DESC LIMIT 1", tableName, version)
	e := db.QueryRow(q).Scan(&row.TStamp, &row.IsApplied)

	if e != nil && e != sql.ErrNoRows {
		log.Fatal(e)
	}

	var appliedAt string

	if row.IsApplied {
		appliedAt = row.TStamp.Format(time.ANSIC)
	} else {
		appliedAt = "Pending"
	}

	log.Printf("    %-24s -- %v\n", appliedAt, script)
}

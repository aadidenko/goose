package goose

import (
	"database/sql"
	"log"
	"sort"
)

// Reset rolls back all migrations
func Reset(db *sql.DB, dir string, tableName string) error {
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}
	statuses, err := dbMigrationsStatus(db, tableName)
	if err != nil {
		return err
	}
	sort.Sort(sort.Reverse(migrations))

	for _, migration := range migrations {
		if !statuses[migration.Version] {
			continue
		}
		if err = migration.Down(db, tableName); err != nil {
			return err
		}
	}

	return nil
}

func dbMigrationsStatus(db *sql.DB, tableName string) (map[int64]bool, error) {
	rows, err := GetDialect().dbVersionQuery(db, tableName)
	if err != nil {
		return map[int64]bool{}, createVersionTable(db, tableName)
	}
	defer rows.Close()

	// The most recent record for each migration specifies
	// whether it has been applied or rolled back.

	result := make(map[int64]bool)

	for rows.Next() {
		var row MigrationRecord
		if err = rows.Scan(&row.VersionID, &row.IsApplied); err != nil {
			log.Fatal("error scanning rows:", err)
		}

		if _, ok := result[row.VersionID]; ok {
			continue
		}

		result[row.VersionID] = row.IsApplied
	}

	return result, nil
}

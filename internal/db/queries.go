package db

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"inserto-paralelo/internal/model"
)

func tableExists(db *sqlx.DB, tableName string) (bool, error) {
	query := `SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = $1
    )`

	var exists bool
	err := db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("Error checking table existence: %v", err)
	}

	return exists, nil
}

func dropTable(db *sqlx.DB) error {
	_, err := db.Exec("drop table checkins")
	if err != nil {
		return fmt.Errorf("Error dropping table 'checkins': %v", err)
	}

	return nil
}

func CreateBook(db *sqlx.DB) error {
	sqlStmt := `
CREATE TABLE checkins (
    UserID TEXT,
    TweetID TEXT,
    Lat REAL,
    Long REAL,
    Time DATETIME,
    VenueID TEXT,
    Text TEXT
)`

	tableName := "checkins"
	exists, err := tableExists(db, tableName)
	if err != nil {
		return fmt.Errorf("unexpected error: %v", err)
	}

	if exists {
		fmt.Printf("Table %s already exists\n", tableName)

		return nil
	}

	// Use sqlx.Exec for prepared statements
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return fmt.Errorf("Error creating table '%s': %v", tableName, err)
	}

	fmt.Printf("Table %s created successfully\n", tableName)
	return nil
}

func insertBatch(db *sqlx.DB, batch []model.Checkin) error {
	// Build the insert statement
	query := `
        INSERT INTO checkins (
            user_id,
            tweet_id,
            lat,
            long,
            time,
            venue_id,
            text
        )
        VALUES
        (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7
        )
    `

	// Prepare the statement
	stmt, err := db.Prepare(query)
	if err != nil {
		return err
	}

	// Bind the values
	for _, checkin := range batch {
		_, err = stmt.Exec(
			checkin.UserID,
			checkin.TweetID,
			checkin.Lat,
			checkin.Long,
			checkin.Time,
			checkin.VenueID,
			checkin.Text,
		)
		if err != nil {
			return err
		}
	}

	// Close the statement
	stmt.Close()

	return nil
}

func InsertCheckinsInBatches(db *sqlx.DB, checkins <-chan model.Checkin) error {
	batchSize := 4000
	batch := make([]model.Checkin, 0, batchSize)

	for checkin := range checkins {
		// Add the checkin to the slice
		batch = append(batch, checkin)

		// If the slice is full, insert the batch
		if len(batch) >= batchSize {
			err := insertBatch(db, batch)
			if err != nil {
				return err
			}

			// Clear the slice
			batch = make([]model.Checkin, 0, batchSize)
		}
	}

	// If there are any remaining checkins, insert them
	if len(batch) > 0 {
		err := insertBatch(db, batch)
		if err != nil {
			return err
		}
	}

	return nil
}

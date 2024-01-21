package db

import (
	"fmt"
	"inserto-paralelo/internal/model"
	"regexp"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
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

func replaceSQL(old, searchPattern string) string {
	re := regexp.MustCompile(regexp.QuoteMeta(searchPattern))
	m := 1

	return re.ReplaceAllStringFunc(old, func(match string) string {
		result := "$" + strconv.Itoa(m)
		m++
		return result
	})
}

func executeBatchInsert(db *sql.DB, sqlString string, args []interface{}) error {
	sqlString = strings.TrimSuffix(sqlString, ",")
	sqlString = replaceSQL(sqlString, "?")

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(sqlString)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(args...)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

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

func InsertCheckinsInBatches(db *sql.DB, checkins <-chan model.Checkin) error {
	fmt.Println("Dentro da função de insert")

	var args []interface{}
	sqlString := "INSERT INTO checkins (UserID, TweetID, Lat, Long, Time, VenueID, Text) VALUES"
	batchSize := 2000
	batchCount := 0

	// Use a loop to read from the channel until it's closed
	for {
		checkin, ok := <-checkins
		if !ok {
			break // Channel closed, exit the loop
		}

		sqlString += "(?,?,?,?,?,?,?),"
		args = append(args,
			checkin.UserID,
			checkin.TweetID,
			checkin.Lat,
			checkin.Long,
			checkin.Time,
			checkin.VenueID,
			checkin.Text)

		// Check if the batch size is reached
		if len(args)/7 == batchSize {
			err := executeBatchInsert(db, sqlString, args)
			if err != nil {
				return err
			}

			// Reset variables for the next batch
			sqlString = "INSERT INTO checkins (UserID, TweetID, Lat, Long, Time, VenueID, Text) VALUES"
			args = []interface{}{}
			batchCount++
		}
	}

	// Insert the remaining data
	if len(args) > 0 {
		err := executeBatchInsert(db, sqlString, args)
		if err != nil {
			return err
		}
	}

	fmt.Printf("%d batches inserted\n", batchCount)

	return nil
}

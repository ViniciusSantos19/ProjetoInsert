package db

import (
	"database/sql"
	"fmt"
	"inserto-paralelo/internal/model"
	"strconv"
	"strings"
)

func tableExists(db *sql.DB, tableName string) (bool, error) {
	query := fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';", tableName)

	rows, err := db.Query(query)
	if err != nil {
		return false, fmt.Errorf("Erro ao verificar a existência da tabela: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil // A tabela existe
	}

	return false, nil // A tabela não existe
}

func CreateBook(db *sql.DB) error {
	sqlStmt := `
CREATE TABLE checkins (
    UserID TEXT,
    TweetID TEXT,
    Lat REAL,
    Long REAL,
    Time DATETIME,
    VenueID TEXT,
    Text TEXT
);`

	tableName := "checkins"
	exists, err := tableExists(db, tableName)

	if err != nil {
		return fmt.Errorf("erro inesperado: %v", err)
	}

	if exists {
		fmt.Printf("A tabela %s já existe\n", tableName)
		return nil
	}

	_, err = db.Exec(sqlStmt)
	if err != nil {
		return fmt.Errorf("Erro ao criar a tabela '%s': %v", tableName, err)
	}

	fmt.Printf("A tabela %s foi criada com sucesso\n", tableName)
	return nil

}

func replaceSQL(old, searchPattern string) string {
	tmpCount := strings.Count(old, searchPattern)
	for m := 1; m <= tmpCount; m++ {
		old = strings.Replace(old, searchPattern, "$"+strconv.Itoa(m), 1)
	}
	return old
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

func InsertCheckinsInBatches(db *sql.DB, checkins <-chan model.Checkin) error {
	fmt.Println("Dentro da função de insert")

	var args []interface{}
	sqlString := "INSERT INTO checkins (UserID, TweetID, Lat, Long, Time, VenueID, Text) VALUES"
	batchSize := 1500
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

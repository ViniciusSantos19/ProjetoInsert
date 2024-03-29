package db

import (
	"database/sql"
	"fmt"
	"inserto-paralelo/internal/model"
	"strings"
)

func CreateCheckin(db *sql.DB) error {
	sqlStmt := `
DROP TABLE IF EXISTS checkins;
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

	_, err := db.Exec(sqlStmt)
	if err != nil {
		return fmt.Errorf("Erro ao criar a tabela '%s': %v", tableName, err)
	}

	fmt.Printf("A tabela %s foi criada com sucesso\n", tableName)
	return nil

}

func buildInsertSQLBatch(checkins []model.Checkin) (string, []interface{}) {
	var sqlString strings.Builder
	var args []interface{}

	sqlString.WriteString("INSERT INTO checkins (UserID, TweetID, Lat, Long, Time, VenueID, Text) VALUES ")

	for i, checkin := range checkins {
		sqlString.WriteString("(?, ?, ?, ?, ?, ?, ?)")

		args = append(args,
			checkin.UserID,
			checkin.TweetID,
			checkin.Lat,
			checkin.Long,
			checkin.Time,
			checkin.VenueID,
			checkin.Text)

		if i < len(checkins)-1 {
			sqlString.WriteString(", ")
		}
	}

	return sqlString.String(), args
}

func InsertCheckinsInBatches(db *sql.DB, checkins <-chan model.Checkin) error {
	fmt.Println("Dentro da função de insert")

	batchSize := 4000
	batchCount := 0

	// Iniciar uma única transação
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			// Recuperar de pânico
			tx.Rollback()
			fmt.Println("Pânico recuperado:", p)
		}

		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
		if err != nil {
			fmt.Println("Erro ao fazer commit:", err)
		}
	}()

	var checkinBatch []model.Checkin

	for checkin := range checkins {
		checkinBatch = append(checkinBatch, checkin)

		if len(checkinBatch) == batchSize {
			sqlString, args := buildInsertSQLBatch(checkinBatch)
			_, err := tx.Exec(sqlString, args...)
			if err != nil {
				return err
			}

			checkinBatch = nil
			batchCount++
		}
	}

	if len(checkinBatch) > 0 {
		sqlString, args := buildInsertSQLBatch(checkinBatch)
		_, err := tx.Exec(sqlString, args...)
		if err != nil {
			return err
		}
		batchCount++
	}

	fmt.Printf("%d batches inserted\n", batchCount)
	return nil
}

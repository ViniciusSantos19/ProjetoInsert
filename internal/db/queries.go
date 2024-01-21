package db

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"inserto-paralelo/internal/model"
)

func CreateCheckins(db *sqlx.DB) error {
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

	// Use sqlx.Exec for prepared statements
	_, err := db.Exec(sqlStmt)
	if err != nil {
		return fmt.Errorf("Error creating table checkins: %v", err)
	}

	fmt.Printf("Table checkins created successfully\n")
	return nil
}

func insertBatch(db *sqlx.DB, batch []model.Checkin) error {
	// Construa a consulta SQL nomeada
	query := "INSERT INTO checkins (UserID, TweetID, Lat, Long, Time, VenueID, Text) VALUES (:UserID, :TweetID, :Lat, :Long, :Time, :VenueID, :Text)"

	// Inicie uma transação
	tx, err := db.Beginx()
	if err != nil {
		fmt.Printf("O erro foi %v", err)
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			// Rollback a transação em caso de pânico
			tx.Rollback()
			panic(p) // Re-levanta o pânico após rollback
		}
	}()

	// Executar a inserção em lote usando NamedExec
	_, err = tx.NamedExec(query, batch)
	if err != nil {
		fmt.Printf("O erro foi %v", err)
		// Se ocorrer um erro, rollback e retorne o erro
		tx.Rollback()
		return err
	}

	// Commit a transação se tudo ocorrer bem
	err = tx.Commit()
	if err != nil {
		fmt.Printf("O erro foi %v", err)
		return err
	}
	return nil
}

func InsertCheckinsInBatches(db *sqlx.DB, checkins <-chan model.Checkin) error {
	batchSize := 4500
	batch := make([]model.Checkin, 0, batchSize)

	for checkin := range checkins {
		batch = append(batch, checkin)

		// If the slice is full, insert the batch
		if len(batch) == batchSize {
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

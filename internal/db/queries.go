package db

import (
	"database/sql"
	"fmt"
	"inserto-paralelo/internal/model"
	"regexp"
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
	re := regexp.MustCompile(regexp.QuoteMeta(searchPattern))
	m := 1

	return re.ReplaceAllStringFunc(old, func(match string) string {
		result := "$" + strconv.Itoa(m)
		m++
		return result
	})
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

	batchSize := 3000
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

		// Commit no final do processamento
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
		if err != nil {
			fmt.Println("Erro ao fazer commit:", err)
		}
	}()

	// Use um loop para ler do canal até que seja fechado
	var checkinBatch []model.Checkin
	for checkin := range checkins {
		checkinBatch = append(checkinBatch, checkin)

		// Verificar se o tamanho do lote foi atingido
		if len(checkinBatch) == batchSize {
			sqlString, args := buildInsertSQLBatch(checkinBatch)
			_, err := tx.Exec(sqlString, args...)
			if err != nil {
				return err
			}

			// Resetar slice para o próximo lote
			checkinBatch = nil
			batchCount++
		}
	}

	// Inserir os dados restantes
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

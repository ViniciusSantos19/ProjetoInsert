package db

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

func ConctarAoBancoDeDados(nomeBancodeDados string) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite3", nomeBancodeDados)

	if err != nil {
		return nil, fmt.Errorf("Erro ao abrir o banco de dados: %v", err)
	}

	//	defer db.Close()

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("Erro ao conectar ao bacno de dados: %v", err)
	}

	fmt.Println("Conexão com o banco de dados estabelecida com sucesso!")

	return db, nil

}

package main

import (
	"fmt"
	"inserto-paralelo/cmd/file"
	"inserto-paralelo/internal/db"
	"log"
	"time"
)

func main() {
	startedAt := time.Now()
	nomeBanco := "../../mydatabase.db"
	dataBase, err := db.ConctarAoBancoDeDados(nomeBanco)
	if err != nil {
		log.Fatal(err)
	}

	caminhoArquivo := "../../checkin_data_foursquare.txt"

	defer dataBase.Close()

	db.CreateCheckin(dataBase)
	fmt.Println("Entrando na funcao pricipal de ler e inserir de forma concorrente")
	file.ReadFromFileConcurrently(caminhoArquivo, dataBase)
	took := time.Since(startedAt)
	fmt.Println(took)
}

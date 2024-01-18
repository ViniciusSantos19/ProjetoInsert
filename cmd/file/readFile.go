package file

import (
	"bufio"
	"database/sql"
	"fmt"
	"inserto-paralelo/internal/db"
	"inserto-paralelo/internal/model"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func convertToFloat64(s string) float64 {
	result, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Printf("Erro ao converter %s para float64: %v", s, err)
	}
	return result
}

func readLines(filePath string, lines chan<- string) {
	defer close(lines)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		lines <- line
		lineCount++
		fmt.Println(lineCount)
	}
	fmt.Println(lineCount)
	fmt.Println("Travou aqui")
	defer fmt.Println("Fecou o canal de lines")
}

func processLines(lines <-chan string, results chan<- model.Checkin) {
	defer close(results)
	for line := range lines {
		fmt.Println("Entrando no loop novamente")

		values := strings.Split(line, "\t")

		checkin := model.Checkin{
			UserID:  values[0],
			TweetID: values[1],
			Lat:     convertToFloat64(values[2]),
			Long:    convertToFloat64(values[3]),
			Time:    parseTime(values[4]),
			VenueID: values[5],
			Text:    values[6],
		}
		fmt.Println("Travou aqui")
		fmt.Printf("O tamanho do buffer é %d\n", len(results))
		results <- checkin
	}
}

func ReadFromFileConcurrently(filePath string, dataBase *sql.DB) {
	var wg sync.WaitGroup
	results := make(chan model.Checkin, 1000)
	line := make(chan string, 100)

	wg.Add(3) // Account for both goroutines

	fmt.Println("Inicio da rotina de ler linhas")
	go func() {
		defer wg.Done()
		readLines(filePath, line)
	}()
	fmt.Println("Inicio da rotina de processar linhas")
	go func() {
		defer wg.Done()
		processLines(line, results)
	}()

	fmt.Println("Inicio da rotina de insertir ao banco de dados")
	go func() {
		defer wg.Done()
		db.InsertCheckinsInBatches(dataBase, results)
	}()

	wg.Wait() // Ensure both goroutines finish

}

func parseTime(s string) time.Time {
	result, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		log.Printf("Erro ao analisar o tempo %s: %v", s, err)
	}
	return result
}

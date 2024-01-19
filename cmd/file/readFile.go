package file

import (
	"bufio"
	"database/sql"
	"fmt"
	//"inserto-paralelo/internal/db"
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

func readLines(filePath string, results chan<- model.Checkin) {
	defer close(results)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		if lineCount >= 1000000 {
			break
		}
		line := scanner.Text()
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
		lineCount++
		results <- checkin
		fmt.Println(lineCount)
	}
	fmt.Println(lineCount)
	fmt.Println("Travou aqui")
	defer fmt.Println("Fecou o canal de lines")
}

func ReadFromFileConcurrently(filePath string, dataBase *sql.DB) {
	var wg sync.WaitGroup
	results := make(chan model.Checkin, 1000)

	wg.Add(2) // Account for both goroutines

	go func() {
		defer wg.Done()
		readLines(filePath, results)
	}()

	go func() {
		defer wg.Done()
		// db.InsertCheckinsInBatches(dataBase, results)
		for checkin := range results {
			fmt.Println("Processou linha")
			fmt.Println(checkin)
		}
	}()
	wg.Wait() // Ensure both goroutines finish
	fmt.Println("Terminou de processar as rotinas")
	fmt.Println(len(results))

}

func parseTime(s string) time.Time {
	result, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		log.Printf("Erro ao analisar o tempo %s: %v", s, err)
	}
	return result
}

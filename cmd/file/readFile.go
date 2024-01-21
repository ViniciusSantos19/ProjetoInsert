package file

import (
	"bufio"
	"fmt"
	"inserto-paralelo/internal/db"
	"inserto-paralelo/internal/model"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
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
		if len(values) == 7 {
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
	}
	fmt.Println(lineCount)
}

func ReadFromFileConcurrently(filePath string, dataBase *sqlx.DB) {
	var wg sync.WaitGroup
	results := make(chan model.Checkin)

	wg.Add(2) // Account for both goroutines

	go func() {
		defer wg.Done()
		readLines(filePath, results)
	}()

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

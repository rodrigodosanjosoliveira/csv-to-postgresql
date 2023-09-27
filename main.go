package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"sync"

	"log"
	"os"

	_ "github.com/lib/pq"
	progressbar "github.com/schollz/progressbar/v3"
)

func main() {
	connStr := "user=postgres password=test1234 host=localhost port=5432 dbname=mydb sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if len(os.Args) != 2 {
		fmt.Println("Usage: ./csv-postgresql <csv-file-path>")
		return
	}

	csvFilePath := os.Args[1]

	csvFile, err := os.Open(csvFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)

	firstLineSkipped := false

	fileInfo, _ := csvFile.Stat()
	fileSize := fileInfo.Size()
	bar := progressbar.NewOptions(
		int(fileSize),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("Processing CSV file"),
	)

	var wg sync.WaitGroup
	maxGoroutines := 4

	sem := make(chan struct{}, maxGoroutines)

	for {
		sem <- struct{}{}

		record, err := csvReader.Read()

		// Check for end of file
		if err != nil {
			if err.Error() == "EOF" {
				break // End of file
			} else {
				log.Fatalf("Error reading CSV file: %v", err)
			}
		}

		if !firstLineSkipped {
			firstLineSkipped = true
			continue
		}

		column1 := record[0]
		column2 := record[1]

		wg.Add(1)
		go func() {
			defer func() {
				<-sem
				wg.Done()
				bar.Add(len(record[0]) + len(record[1]))
			}()

			_, err = db.Exec("INSERT INTO braze(column1, column2) VALUES ($1, $2)", column1, column2)
			if err != nil {
				log.Fatal(err)
			}
		}()
	}

	wg.Wait()

	fmt.Println("Data imported successfully")

}

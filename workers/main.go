package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/LucasAlda/demo-falopa/middleware"
)

// const demo = "REVIEW"

func main() {
	middleware, err := middleware.NewMiddleware()
	if err != nil {
		panic(err)
	}
	defer middleware.Close()

	os.Mkdir("./database", 0777)

	stats := make(chan int)
	go writeStats(stats)

	processGames(middleware, stats)

	validate()

	// time.Sleep(10000 * time.Second)
}

type Game struct {
	AppId int  `json:"app_id"`
	Score int  `json:"score"`
	Last  bool `json:"last"`
}

func writeStats(stats <-chan int) error {
	i := 0
	lastTime := time.Now()
	lastLog := 0

	for stat := range stats {
		i += stat
		if (i-lastLog) > 50_000 && i > 1000 {
			now := time.Now()
			diff := now.Sub(lastTime).Milliseconds()
			if diff > 0 {
				fmt.Printf("[updating] Game:  %d, Time: %dms, Speed: %f u/ms\n", i, diff, float64(i-lastLog)/float64(diff))
			}
			lastTime = now
			lastLog = i
		}
	}
	return nil
}

func updateGame(game *middleware.Stats) error {
	os.MkdirAll("./database/cliente", 0777)
	file, err := os.Open(fmt.Sprintf("./database/cliente/%d.csv", game.AppId))
	if err == nil {
		reader := csv.NewReader(file)

		record, err := reader.Read()
		if err != nil && err != io.EOF {
			log.Fatalf("failed to read line: %v", err)
		}

		positives, err := strconv.Atoi(record[2])
		if err != nil {
			log.Fatalf("failed to convert positives to int: %v", err)
		}

		negatives, err := strconv.Atoi(record[3])
		if err != nil {
			log.Fatalf("failed to convert negatives to int: %v", err)
		}

		game.Positives += positives
		game.Negatives += negatives

		_, err = file.Seek(0, 0)
		if err != nil {
			log.Fatalf("failed to seek to start of file: %v", err)
		}
	}
	defer file.Close()

	tmp, err := os.CreateTemp("./database", "*.csv")
	if err != nil {
		log.Fatalf("failed to create temp file: %v", err)
	}

	writer := csv.NewWriter(tmp)

	err = writer.Write([]string{strconv.Itoa(game.AppId), game.Name, strconv.Itoa(game.Positives), strconv.Itoa(game.Negatives)})
	if err != nil {
		log.Fatalf("failed to write to file: %v", err)
	}

	writer.Flush()

	err = os.Rename(tmp.Name(), fmt.Sprintf("./database/cliente/%d.csv", game.AppId))
	if err != nil {
		log.Fatalf("failed to rename file: %v", err)
	}

	return nil
}

func processGames(m *middleware.Middleware, stats chan<- int) error {
	queue, err := m.ListenStats(os.Getenv("ID"), "Action")
	if err != nil {
		return err
	}

	i := 0
	queue.Consume(func(message *middleware.StatsMsg, ack func()) error {
		i++
		if i > 10000 && i > 0 {
			stats <- i
			i = 0
		}

		updateGame(message.Stats)
		ack()
		return nil
	})

	return nil
}

func validate() error {
	positivesCounter := make(chan int)
	negativesCounter := make(chan int)

	go func() {
		id, err := strconv.Atoi(os.Getenv("ID"))
		if err != nil {
			log.Fatalf("failed to convert ID to int: %v", err)
		}

		for i := id; i < 10_000; i += 3 {
			file, err := os.Open(fmt.Sprintf("./database/cliente/%d.csv", i))
			if err != nil {
				log.Fatalf("failed to open file: %v", err)
			}

			reader := csv.NewReader(file)

			record, err := reader.Read()
			if err != nil && err != io.EOF {
				log.Fatalf("failed to read line: %v", err)
			}

			positives, err := strconv.Atoi(record[2])
			if err != nil {
				log.Fatalf("failed to convert positives to int: %v", err)
			}

			negatives, err := strconv.Atoi(record[3])
			if err != nil {
				log.Fatalf("failed to convert negatives to int: %v", err)
			}

			positivesCounter <- positives
			negativesCounter <- negatives
		}

		close(positivesCounter)
		close(negativesCounter)
	}()

	totalPositives := 0
	for positives := range positivesCounter {
		totalPositives += positives
	}

	totalNegatives := 0
	for negatives := range negativesCounter {
		totalNegatives += negatives
	}
	fmt.Printf("Total Validated: %d\n", totalPositives+totalNegatives)
	fmt.Printf("Total Positives: %d\n", totalPositives)
	fmt.Printf("Total Negatives: %d\n", totalNegatives)

	return nil
}

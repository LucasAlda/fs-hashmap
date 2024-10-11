package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

const demo = "REVIEW"

func main() {
	id := os.Getenv("ID")

	os.Mkdir("./database", 0755)
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	ch.ExchangeDeclare(
		"games", // name
		"topic", // type
		true,    // durable
		false,   // delete when unused
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)

	queue, err := ch.QueueDeclare(
		"",    // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	ch.QueueBind(
		queue.Name,                 // queue name
		fmt.Sprintf("game.%s", id), // routing key
		"games",                    // exchange
		false,                      // no-wait
		nil,                        // arguments
	)

	if err != nil {
		panic(err)
	}

	stats := make(chan int)
	go writeStats(stats)

	processGames(ch, &queue, stats)

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

func updater(msgs <-chan amqp.Delivery, stats chan<- int) error {
	i := 0

	for msg := range msgs {
		game := Game{}
		err := json.Unmarshal(msg.Body, &game)
		if err != nil {
			log.Fatalf("failed to unmarshal game: %v", err)
		}

		if game.Last {
			break
		}

		i++
		if i > 10000 && i > 0 {
			stats <- i
			i = 0
		}

		if demo == "GAME" {
			updateGameOnce(game)
		} else {
			updateGame(game)
		}

		msg.Ack(false)

	}

	return nil
}

func updateGame(game Game) error {
	file, err := os.OpenFile(fmt.Sprintf("./database/%d.csv", game.AppId), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		log.Fatalf("failed to get file stat: %v", err)
	}

	if stat.Size() > 0 {
		reader := bufio.NewReader(file)

		line, _, err := reader.ReadLine()
		if err != nil && err != io.EOF {
			log.Fatalf("failed to read line: %v", err)
		}

		parts := strings.Split(string(line), ",")
		score, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Fatalf("failed to convert score to int: %v", err)
		}

		game.Score += score
	}

	if err != nil {
		log.Fatalf("failed to create temp file: %v", err)
	}
	writer := bufio.NewWriter(file)

	_, err = file.Seek(0, 0)
	if err != nil {
		log.Fatalf("failed to seek to start of file: %v", err)
	}

	_, err = writer.WriteString(fmt.Sprintf("%d,%d\n", game.AppId, game.Score))
	if err != nil {
		log.Fatalf("failed to write to file: %v", err)
	}

	if err := writer.Flush(); err != nil {
		log.Fatalf("failed to flush writer: %v", err)
	}

	return nil
}

func updateGameOnce(game Game) error {
	file, err := os.OpenFile(fmt.Sprintf("./database/%d.csv", game.AppId), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		log.Fatalf("failed to get file stat: %v", err)
	}

	if stat.Size() > 0 {
		reader := bufio.NewReader(file)

		line, _, err := reader.ReadLine()
		if err != nil && err != io.EOF {
			log.Fatalf("failed to read line: %v", err)
		}

		parts := strings.Split(string(line), ",")
		score, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Fatalf("failed to convert score to int: %v", err)
		}

		game.Score += score
		return nil
	}

	if err != nil {
		log.Fatalf("failed to create temp file: %v", err)
	}
	writer := bufio.NewWriter(file)

	_, err = file.Seek(0, 0)
	if err != nil {
		log.Fatalf("failed to seek to start of file: %v", err)
	}

	_, err = writer.WriteString(fmt.Sprintf("%d,%d\n", game.AppId, game.Score))
	if err != nil {
		log.Fatalf("failed to write to file: %v", err)
	}

	if err := writer.Flush(); err != nil {
		log.Fatalf("failed to flush writer: %v", err)
	}

	return nil
}

func processGames(ch *amqp.Channel, queue *amqp.Queue, stats chan<- int) error {
	msgs, err := ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	updater(msgs, stats)

	return nil
}

func validate() error {
	counter := make(chan int)

	go func() {
		id, err := strconv.Atoi(os.Getenv("ID"))
		if err != nil {
			log.Fatalf("failed to convert ID to int: %v", err)
		}

		for i := id; i < 10_000; i += 3 {

			file, err := os.Open(fmt.Sprintf("./database/%d.csv", i))
			if err != nil {
				log.Fatalf("failed to open file: %v", err)
			}

			reader := bufio.NewReader(file)

			line, _, err := reader.ReadLine()
			if err != nil && err != io.EOF {
				log.Fatalf("failed to read line: %v", err)
			}

			parts := strings.Split(string(line), ",")
			score, err := strconv.Atoi(parts[1])
			if err != nil {
				log.Fatalf("failed to convert score to int: %v", err)
			}

			counter <- score
		}

		close(counter)
	}()

	total := 0
	for score := range counter {
		total += score
	}
	fmt.Printf("Total Validated: %d\n", total)

	return nil
}

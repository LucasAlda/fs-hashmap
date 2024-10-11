package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

const CANT_REVIEWS = 10_000_000
const CANT_GAMES = 10_000

func main() {
	// ch := make(chan Game)

	// stats := make(chan int)
	// go seedDB(ch)
	// go writeStats(stats)
	// processGames(ch, stats)

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

	err = ch.ExchangeDeclare(
		"games", // name
		"topic", // type
		true,    // durable
		false,   // delete when unused
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		panic(err)
	}

	time.Sleep(3 * time.Second)

	seedDB(ch)
}

type Game struct {
	AppId int  `json:"app_id"`
	Score int  `json:"score"`
	Last  bool `json:"last"`
}

func seedDB(ch *amqp.Channel) error {

	for i := 0; i < CANT_REVIEWS; i++ {
		if i%25_000 == 0 {
			fmt.Printf("Seeding %d reviews\n", i)
		}

		appId := rand.Intn(CANT_GAMES)
		body := Game{
			AppId: appId,
			Score: 1,
			Last:  false,
		}
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return err
		}

		topic := appId % 3

		err = ch.Publish(
			"games",                       // exchange
			fmt.Sprintf("game.%d", topic), // routing key
			false,                         // mandatory
			false,                         // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        bodyBytes,
			})
		if err != nil {
			return err
		}
	}

	for i := 0; i < 3; i++ {
		body := Game{
			AppId: -1,
			Score: 0,
			Last:  true,
		}

		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return err
		}

		err = ch.Publish(
			"games",                   // exchange
			fmt.Sprintf("game.%d", i), // routing key
			false,                     // mandatory
			false,                     // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        bodyBytes,
			})
		if err != nil {
			return err
		}
	}

	return nil
}

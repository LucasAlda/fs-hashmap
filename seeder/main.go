package main

import (
	"fmt"
	"log"
	"time"

	"github.com/LucasAlda/demo-falopa/middleware"
)

const CANT_REVIEWS = 30_000

const CANT_GAMES = 10_000

func main() {
	middleware, err := middleware.NewMiddleware()
	if err != nil {
		panic(err)
	}

	defer middleware.Close()

	time.Sleep(5 * time.Second)

	seedDB(middleware)
}

type Game struct {
	AppId int  `json:"app_id"`
	Score int  `json:"score"`
	Last  bool `json:"last"`
}

func seedDB(m *middleware.Middleware) error {

	for i := 1; i <= CANT_REVIEWS; i++ {
		if i%10_000 == 0 {
			fmt.Printf("Seeding %d reviews\n", i)
		}

		body := middleware.StatsMsg{
			Id: i,
			Stats: &middleware.Stats{
				AppId:     1,
				Name:      "Really Long Game Name here 2077: Deluxe Edition",
				Text:      "This is a review text",
				Genres:    []string{"Action"},
				Positives: 1,
				Negatives: 0,
			},
		}

		err := m.SendStats(&body)
		if err != nil {
			return err
		}
	}

	log.Printf("Sending finished message, sent %d msgs", CANT_REVIEWS)
	err := m.SendStatsFinished()
	if err != nil {
		return err
	}

	return nil
}

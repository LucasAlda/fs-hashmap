package main

import (
	"fmt"

	"math/rand"

	"github.com/LucasAlda/demo-falopa/middleware"
)

const CANT_REVIEWS = 3_000_000

const CANT_GAMES = 10_000

func main() {
	middleware, err := middleware.NewMiddleware()
	if err != nil {
		panic(err)
	}

	defer middleware.Close()

	seedDB(middleware)
}

type Game struct {
	AppId int  `json:"app_id"`
	Score int  `json:"score"`
	Last  bool `json:"last"`
}

func seedDB(m *middleware.Middleware) error {

	for i := 0; i < CANT_REVIEWS; i++ {
		if i%25_000 == 0 {
			fmt.Printf("Seeding %d reviews\n", i)
		}

		body := middleware.StatsMsg{
			Stats: &middleware.Stats{
				AppId:     rand.Intn(CANT_GAMES),
				Name:      "Really Long Game Name here 2077: Deluxe Edition",
				Text:      "This is a review text",
				Genres:    []string{"Action", "Adventure"},
				Positives: 1,
				Negatives: 1,
			},
		}

		err := m.SendStats(&body)
		if err != nil {
			return err
		}
	}

	for i := 0; i < 3; i++ {

		err := m.SendStatsFinished()
		if err != nil {
			return err
		}
	}

	return nil
}

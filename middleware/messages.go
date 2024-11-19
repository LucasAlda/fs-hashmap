package middleware

import (
	"slices"
	"strconv"
	"strings"
)

type Game struct {
	AppId       int
	Name        string
	Year        int
	Genres      []string
	Windows     bool
	Mac         bool
	Linux       bool
	AvgPlaytime int64
}

const appIdIndex = 0
const nameIndex = 1
const yearIndex = 2
const genreIndex = 36
const windowsIndex = 17
const macIndex = 18
const linuxIndex = 19
const avgPlaytimeIndex = 29

func NewGame(record []string) *Game {
	appId, err := strconv.Atoi(record[appIdIndex])
	if err != nil {
		return nil
	}

	year, err := strconv.Atoi(record[yearIndex][len(record[yearIndex])-4:])
	if err != nil {
		return nil
	}

	avgPlaytime, err := strconv.ParseInt(record[avgPlaytimeIndex], 10, 64)
	if err != nil {
		return nil
	}
	game := &Game{
		AppId:       appId,
		Name:        record[nameIndex],
		Year:        year,
		Genres:      strings.Split(record[genreIndex], ","),
		Windows:     record[windowsIndex] == "True",
		Mac:         record[macIndex] == "True",
		Linux:       record[linuxIndex] == "True",
		AvgPlaytime: avgPlaytime,
	}
	return game
}

type GameMsg struct {
	Game *Game
	Last bool
}

type Review struct {
	AppId string
	Text  string
	Score int
}

const appIdIndexReview = 0
const textIndexReview = 2
const scoreIndexReview = 3

func NewReview(record []string) *Review {
	score, err := strconv.Atoi(record[scoreIndexReview])
	if err != nil {
		return nil
	}
	return &Review{
		AppId: record[appIdIndexReview],
		Text:  record[textIndexReview],
		Score: score,
	}
}

type ReviewsBatch struct {
	Reviews []Review
	Last    int
}

type Stats struct {
	AppId     int
	Name      string
	Text      string
	Genres    []string
	Positives int
	Negatives int
}

func NewStats(game []string, review *Review) *Stats {
	appId, err := strconv.Atoi(game[appIdIndex])
	if err != nil {
		return nil
	}

	genres := strings.Split(game[3], ",")
	if !slices.Contains(genres, "Action") {
		review.Text = ""
	}

	if review.Score > 0 {
		return &Stats{
			AppId:     appId,
			Name:      game[1],
			Genres:    genres,
			Text:      review.Text,
			Positives: 1,
			Negatives: 0,
		}
	}

	return &Stats{
		AppId:     appId,
		Name:      game[1],
		Genres:    genres,
		Text:      review.Text,
		Positives: 0,
		Negatives: 1,
	}
}

type StatsMsg struct {
	Stats *Stats
	Last  bool
}

type Result struct {
	QueryId        int
	IsFinalMessage bool
	Payload        interface{}
}

type Query1Result struct {
	Windows int64
	Mac     int64
	Linux   int64
	Final   bool
}

type Query2Result struct {
	TopGames []Game
}

type Query3Result struct {
	TopStats []Stats
}

type Query4Result struct {
	Game string
}

type Query5Result struct {
	Stats []Stats
	// GamesNeeded int
}

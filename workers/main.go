package main

import (
	"encoding/binary"
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

var statsCache = make(map[int]middleware.Stats)

func main() {
	middleware, err := middleware.NewMiddleware()
	if err != nil {
		panic(err)
	}
	defer middleware.Close()

	os.MkdirAll("./database/stats", 0777)

	metrics := make(chan int)
	go writeMetrics(metrics)

	processStats(middleware, metrics)
}

type Game struct {
	AppId int  `json:"app_id"`
	Score int  `json:"score"`
	Last  bool `json:"last"`
}

func writeMetrics(metrics <-chan int) error {
	i := 0
	lastTime := time.Now()
	lastLog := 0

	go func() {
		for {
			time.Sleep(1 * time.Second)
			fmt.Printf("Stats: %d\n", i)
		}
	}()

	for stat := range metrics {
		if i == 0 {
			lastTime = time.Now()
		}

		i += stat
		if (i-lastLog) >= 10_000 && i > 1000 {
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

func updateStat(stat *middleware.Stats, tmpFile *os.File) {
	if cached, ok := statsCache[stat.AppId]; ok {
		stat.Negatives += cached.Negatives
		stat.Positives += cached.Positives
	} else {
		file, err := os.Open(fmt.Sprintf("./database/stats/%d.csv", stat.AppId))
		if err == nil {
			reader := csv.NewReader(file)

			record, err := reader.Read()
			if err != nil && err != io.EOF {
				log.Printf("failed to read line: %v", err)
				return
			}

			positives, err := strconv.Atoi(record[2])
			if err != nil {
				log.Printf("failed to convert positives to int: %v", err)
				return
			}

			negatives, err := strconv.Atoi(record[3])
			if err != nil {
				log.Printf("failed to convert negatives to int: %v", err)
				return
			}

			stat.Positives += positives
			stat.Negatives += negatives
		}

		file.Close()
	}

	writer := csv.NewWriter(tmpFile)

	err := writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Positives), strconv.Itoa(stat.Negatives)})
	if err != nil {
		log.Printf("failed to write to file: %v", err)
		return
	}

	writer.Flush()
	statsCache[stat.AppId] = *stat
}

func updateProcessed(id int) {

	file, err := os.OpenFile("./database/processed.bin", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return
	}

	defer file.Close()

	err = binary.Write(file, binary.BigEndian, int32(id))
	if err != nil {
		log.Printf("failed to write to file: %v", err)
		return
	}

}

func getAlreadyProcessed() map[int]bool {
	alreadyProcessed := make(map[int]bool)
	file, err := os.Open("./database/processed.bin")
	if err != nil {
		return alreadyProcessed
	}

	defer file.Close()

	var current int32

	for {
		err := binary.Read(file, binary.BigEndian, &current)
		if err == io.EOF {
			break
		}

		alreadyProcessed[int(current)] = true
	}

	return alreadyProcessed
}

func processStats(m *middleware.Middleware, metrics chan<- int) error {
	alreadyProcessed := getAlreadyProcessed()

	RestoreCommit(func(commit *Commit) {
		log.Printf("Restoring stat id: %s", commit.data[0][1])

		err := os.Rename(commit.data[0][2], fmt.Sprintf("./database/stats/%s.csv", commit.data[0][0]))

		if err != nil {
			log.Printf("failed to rename file: %v", err)
			return
		}

		id, err := strconv.Atoi(commit.data[0][1])
		if err != nil {
			log.Printf("failed to convert id to int: %v", err)
			return
		}

		if alreadyProcessed[id] {
			commit.end()
			return
		}

		updateProcessed(id)
		alreadyProcessed[id] = true

		commit.end()
	})

	queue, err := m.ListenStats(os.Getenv("ID"), "Action")
	if err != nil {
		return err
	}

	log.Printf("Listening stats")

	finished := false

	queue.Consume(func(message *middleware.StatsMsg, ack func()) error {

		if finished {
			log.Printf("New messages when already finished :(")
			return nil
		}

		if message.Last {
			finished = true
			log.Printf("Last message received")
			ack()
			return nil
		}

		metrics <- 1

		if alreadyProcessed[message.Id] {
			ack()
			return nil
		}

		tmpFile, err := os.CreateTemp("./database", fmt.Sprintf("%d.csv", message.Stats.AppId))
		if err != nil {
			log.Printf("failed to create temp file: %v", err)
			return nil
		}

		data := [][]string{
			{strconv.Itoa(message.Stats.AppId), strconv.Itoa(message.Id), tmpFile.Name()},
		}

		updateStat(message.Stats, tmpFile)

		commit := NewCommit(fmt.Sprintf("%d", message.Stats.AppId), data)

		updateProcessed(message.Id)
		alreadyProcessed[message.Id] = true

		os.Rename(tmpFile.Name(), fmt.Sprintf("./database/stats/%d.csv", message.Stats.AppId))

		commit.end()

		ack()

		return nil
	})

	return nil
}

type Commit struct {
	commit *os.File
	data   [][]string
}

// data: [[filename, tmpFilename],[filename, tmpFilename],[key,value]]

// commitFile:
// filename,tmpFilename
// filename,tmpFilename
// key,value
// END
func NewCommit(path string, data [][]string) *Commit {
	commit, err := os.OpenFile("./database/commit.csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Printf("failed to create commit file: %v", err)
		return nil
	}

	writer := csv.NewWriter(commit)
	writer.WriteAll(data)
	writer.Write([]string{"END"})
	writer.Flush()

	return &Commit{commit: commit, data: data}
}

func RestoreCommit(onCommit func(commit *Commit)) {
	commitFile, err := os.Open("./database/commit.csv")
	if err != nil {
		log.Printf("No commit file found: %v", err)
		return
	}

	reader := csv.NewReader(commitFile)
	reader.FieldsPerRecord = -1

	data, err := reader.ReadAll()
	if err != nil {
		log.Printf("failed to read commit file: %v", err)
		return
	}

	if len(data) == 0 {
		log.Printf("Empty commit file")
		return
	}

	if len(data[len(data)-1]) == 0 {
		log.Printf("Empty commit file (2)")
		return
	}

	if data[len(data)-1][0] != "END" {
		log.Printf("Empty commit file (3)")
		return
	}

	commit := &Commit{commit: commitFile, data: data[:len(data)-1]}

	onCommit(commit)
}

func (c *Commit) end() {
	c.commit.Truncate(0)
	c.commit.Close()
}

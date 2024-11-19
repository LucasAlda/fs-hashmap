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

func main() {
	middleware, err := middleware.NewMiddleware()
	if err != nil {
		panic(err)
	}
	defer middleware.Close()

	os.Mkdir("./database", 0777)

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
	file, err := os.Open(fmt.Sprintf("./database/%d/stat.csv", stat.AppId))
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

		_, err = file.Seek(0, 0)
		if err != nil {
			log.Printf("failed to seek to start of file: %v", err)
			return
		}
	}
	defer file.Close()

	writer := csv.NewWriter(tmpFile)

	err = writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Positives), strconv.Itoa(stat.Negatives)})
	if err != nil {
		log.Printf("failed to write to file: %v", err)
		return
	}

	writer.Flush()
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

	RestoreCommits(func(commit *Commit) {
		log.Printf("Restoring stat...")

		err := os.Rename(commit.data[0][2], fmt.Sprintf("./database/%s/stat.csv", commit.data[0][0]))

		if err != nil {
			log.Printf("failed to rename file: %v", err)
			return
		}

		id, err := strconv.Atoi(commit.data[0][1])
		if err != nil {
			log.Printf("failed to convert id to int: %v", err)
			return
		}

		alreadyProcessed[id] = true
		commit.end()
	})

	queue, err := m.ListenStats(os.Getenv("ID"), "Action")
	if err != nil {
		return err
	}

	log.Printf("Listening stats")

	queue.Consume(func(message *middleware.StatsMsg, ack func()) error {
		metrics <- 1

		os.MkdirAll(fmt.Sprintf("./database/%d", message.Stats.AppId), 0777)

		if alreadyProcessed[message.Id] {
			ack()
			return nil
		}

		tmpFile, err := os.CreateTemp(fmt.Sprintf("./database/%d", message.Stats.AppId), "stat.csv")
		if err != nil {
			log.Printf("failed to create temp file: %v", err)
			return nil
		}

		data := [][]string{
			{strconv.Itoa(message.Stats.AppId), strconv.Itoa(message.Id), tmpFile.Name()},
		}

		commit := NewCommit(fmt.Sprintf("%d", message.Stats.AppId), data)

		updateStat(message.Stats, tmpFile)
		updateProcessed(message.Id)
		alreadyProcessed[message.Id] = true

		os.Rename(tmpFile.Name(), fmt.Sprintf("./database/%d/stat.csv", message.Stats.AppId))

		commit.end()

		ack()

		return nil
	})

	return nil
}

type Commit struct {
	path   string
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
	os.MkdirAll("./database/commit", 0777)
	os.MkdirAll(fmt.Sprintf("./database/%s", path), 0777)
	commit, err := os.Create(fmt.Sprintf("./database/commit/%s.csv", path))
	if err != nil {
		log.Printf("failed to create commit file: %v", err)
		return nil
	}

	writer := csv.NewWriter(commit)
	writer.WriteAll(data)
	writer.Write([]string{"END"})
	writer.Flush()

	return &Commit{path: path, commit: commit, data: data}
}

func RestoreCommits(onCommit func(commit *Commit)) {
	files, err := os.ReadDir("./database/commit/")
	if err != nil {
		return
	}

	for _, file := range files {
		restoreCommit(file.Name(), onCommit)
	}
}

func restoreCommit(path string, onCommit func(commit *Commit)) {
	log.Printf("Restore commit: %s", path)
	commitFile, err := os.Open(fmt.Sprintf("./database/commit/%s", path))
	if err != nil {
		log.Printf("failed to open commit file: %v", err)
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
		log.Printf("empty commit file: %s", path)
		return
	}

	if len(data[len(data)-1]) == 0 {
		log.Printf("empty commit file (2): %s", path)
		return
	}

	if data[len(data)-1][0] == "END" {
		log.Printf("empty commit file (3): %s", path)
		return
	}

	commit := &Commit{path: path, commit: commitFile, data: data[:len(data)-1]}

	onCommit(commit)
}

func (c *Commit) end() {
	os.Remove(c.commit.Name())
}

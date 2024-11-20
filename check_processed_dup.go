package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	file, err := os.Open("../database-falopa/processed.bin")
	if err != nil {
		log.Fatalf("failed to read processed.bin: %v", err)
	}

	// Read all the file int32 and check if there are duplicates

	alreadyProcessed := make(map[int32]bool)

	for {
		var id int32
		err := binary.Read(file, binary.BigEndian, &id)
		if err == io.EOF {
			break
		}

		if _, ok := alreadyProcessed[id]; ok {
			fmt.Println("Duplicado: ", id)
		}
	}

	file.Close()
}

package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	colIdx := 0
	if len(os.Args) > 1 {
		colIdx, _ = strconv.Atoi(os.Args[1])
	}

	counts := make(map[string]int)
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		r := csv.NewReader(strings.NewReader(line))
		r.FieldsPerRecord = -1 // the nubmer of fields per record can vary

		record, err := r.Read()
		key := ""

		if err == nil && colIdx < len(record) {
			key = record[colIdx]
		}

		counts[key]++

		// Continuously output
		fmt.Printf("%s: %d\n", key, counts[key])
	}
}

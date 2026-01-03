package main

import (
	"bufio"
	"encoding/csv"
	"os"
	"strings"
)

// Application 2 Stage 2: Transform
// Requirement: Output fields 1-3 of the line
func main() {
	scanner := bufio.NewScanner(os.Stdin)
	writer := csv.NewWriter(os.Stdout)

	for scanner.Scan() {
		line := scanner.Text()

		r := csv.NewReader(strings.NewReader(line))
		r.FieldsPerRecord = -1
		record, err := r.Read()

		if err == nil && len(record) >= 3 {
			outputFields := record[:3]
			writer.Write(outputFields)
			writer.Flush()
		}
		// Drop when error or not enough fields
	}
}

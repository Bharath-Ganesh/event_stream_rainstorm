package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Application 1: ./op1_filter "Traffic" 1, return "Key\tLine"
// Application 2: ./op1_filter "Traffic", return "Line" w/o tab
func main() {
	if len(os.Args) < 2 {
		return
	}
	pattern := os.Args[1]
	hasColIdx := false
	colIdx := 0
	if len(os.Args) >= 3 {
		if idx, err := strconv.Atoi(os.Args[2]); err == nil {
			colIdx = idx
			hasColIdx = true
		}
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, pattern) {
			// Application 1
			if hasColIdx {
				r := csv.NewReader(strings.NewReader(line))
				r.FieldsPerRecord = -1 // the nubmer of fields per record can vary

				record, err := r.Read()
				key := ""

				// only parse when having valid col index
				if err == nil && colIdx < len(record) {
					key = record[colIdx]
				}
				fmt.Printf("%s\t%s\n", key, line)
			} else {
				// Application 2
				fmt.Println(line)
			}
		}
	}
}

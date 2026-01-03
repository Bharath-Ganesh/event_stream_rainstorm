package client

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	. "gitlab.engr.illinois.edu/yonghan4/mp4_g82/distributed_log_querier/utils"
)

type ConfigStruct struct {
	Name     string `json:"name"`
	Addr     string `json:"address"`
	Filepath string `json:"filepath"`
}

type ResultStruct struct {
	Name      string
	VmAddr    string
	LogOutput []string
	TotalLine int
}

func ReadConfigFile(configpath string) []ConfigStruct {
	configfile, err := os.ReadFile(configpath)
	CheckError(err, "Cannot read config file")
	configStruct := []ConfigStruct{}
	parsejsonerr := json.Unmarshal(configfile, &configStruct)
	CheckError(parsejsonerr, "Cannot parse json file")
	return configStruct
}

func getFromServer(vmconfig ConfigStruct, grepArgs []string, resultChan chan ResultStruct, waitgroup *sync.WaitGroup) {
	defer waitgroup.Done()
	conn, err := net.Dial("tcp", vmconfig.Addr)
	if err != nil {
		// Cannot connet to server
		return
	}
	defer conn.Close()
	grepArgs = append(grepArgs, vmconfig.Filepath)
	grepArgsString := strings.Join(grepArgs, " ")
	_, err = conn.Write([]byte(grepArgsString + "\n"))
	if err != nil {
		//Cannot write to server
		return
	}
	loglines := []string{}
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		loglines = append(loglines, scanner.Text())
	}
	scanerr := scanner.Err()
	if scanerr != nil {
		//Cannot scan the text from server
		return
	}
	if len(loglines) > 0 {
		resultChan <- ResultStruct{
			Name:      vmconfig.Name,
			VmAddr:    vmconfig.Addr,
			LogOutput: loglines,
			TotalLine: len(loglines),
		}
	}
}

func waitandClose(waitgroup *sync.WaitGroup, resultChan chan ResultStruct) {
	waitgroup.Wait()
	close(resultChan)
}

func GetLogandTotalLine(configStruct []ConfigStruct, grepArgs []string, cInArgs bool) ([]ResultStruct, int) {
	resultChan := make(chan ResultStruct)
	waitgroup := sync.WaitGroup{}
	collectResult := []ResultStruct{}
	sumTotalLine := 0
	for _, vmconfig := range configStruct {
		waitgroup.Add(1)
		go getFromServer(vmconfig, grepArgs, resultChan, &waitgroup)
	}
	go waitandClose(&waitgroup, resultChan)
	for eachresult := range resultChan {
		if cInArgs && len(eachresult.LogOutput) > 0 {
			strLineCount := eachresult.LogOutput[0]
			intLineCount, err := strconv.Atoi(strLineCount)
			if err == nil {
				eachresult.TotalLine = intLineCount
			}
		}
		sumTotalLine += eachresult.TotalLine
		collectResult = append(collectResult, eachresult)
	}
	return collectResult, sumTotalLine
}

func main() {
	if len(os.Args) < 3 {
		log.Println("Usage: go run ./client_main/client_main.go <config_file> <grep_args>")
		return
	}
	// Last argument is the config file, rest are grep args
	configFile := os.Args[1]
	grepArgs := os.Args[2:]
	cInArgs := false
	for _, grepargs := range grepArgs {
		if grepargs == "-c" {
			cInArgs = true
			break
		}
	}
	configStruct := ReadConfigFile(configFile)
	start := time.Now()
	collectResult, sumTotalLine := GetLogandTotalLine(configStruct, grepArgs, cInArgs)
	end := time.Since(start)
	log.Println("Here's the result ---------")
	if !cInArgs {
		for _, eachResult := range collectResult {
			for _, eachline := range eachResult.LogOutput {
				log.Printf("[%s] %s\n", eachResult.Name, eachline)
			}
		}
	}
	for _, eachresult := range collectResult {
		log.Printf("[%s] totallines: %d\n", eachresult.Name, eachresult.TotalLine)
	}
	log.Println("total number of matching lines summed across all VMs: ", sumTotalLine)
	log.Println("Latency ", end)
	log.Println("End-------------------------")
}

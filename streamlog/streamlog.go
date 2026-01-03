package streamlog

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type StreamLoggers struct {
	LogFile         *os.File
	InfoLogger      *log.Logger
	OutputLogger    *log.Logger
	DuplicateLogger *log.Logger // [(Exactly-once)
	RateLogger      *log.Logger
	ErrorLogger     *log.Logger
	JobLogger       *log.Logger
	TaskLogger      *log.Logger
	AutoScaleLogger *log.Logger
	FailLogger      *log.Logger
	ReplayLogger    *log.Logger
	DedupLogger     *log.Logger
}

func NewStreamLogger(stageID int, taskID int, prefix string) (*StreamLoggers, string, error) {
	// timestamp format: unique log between tests
	timestamp := time.Now().Format("20060102_150405")
	var filename string

	if prefix != "" {
		// Leader Log
		filename = fmt.Sprintf("%s_%s.log", prefix, timestamp)
	} else {
		// Task Log
		filename = fmt.Sprintf("rs_Stage%d_Task%d_%s.log", stageID, taskID, timestamp)
	}
	streamLogFilePath := "slog"
	os.MkdirAll(streamLogFilePath, 0755)
	filename = filepath.Join(streamLogFilePath, filename)

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, "", err
	}

	logFlag := log.Ldate | log.Ltime | log.Lmicroseconds

	return &StreamLoggers{
		LogFile:         f,
		InfoLogger:      log.New(f, "[INFO] ", logFlag),
		OutputLogger:    log.New(f, "[OUTPUT] ", logFlag),
		DuplicateLogger: log.New(f, "[DUPLICATE] ", logFlag),
		RateLogger:      log.New(f, "[RATE] ", logFlag),
		ErrorLogger:     log.New(f, "[ERROR] ", logFlag),
		JobLogger:       log.New(f, "[JOB] ", logFlag),
		TaskLogger:      log.New(f, "[Task] ", logFlag),
		AutoScaleLogger: log.New(f, "[AutoScale] ", logFlag),
		FailLogger:      log.New(f, "[Fail] ", logFlag),
		ReplayLogger:    log.New(f, "[REPLAY] ", logFlag),
		DedupLogger:     log.New(f, "[DedupLog] ", logFlag),
	}, filename, nil
}

func (sl *StreamLoggers) Close() {
	if sl.LogFile != nil {
		sl.LogFile.Close()
	}
}

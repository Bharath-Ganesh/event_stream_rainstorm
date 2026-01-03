package vmlog

import (
	"log"
	"os"
)

var (
	JoinLogger      *log.Logger
	SuspectedLogger *log.Logger
	FailedLogger    *log.Logger
	RefutedLogger   *log.Logger
	DeletedLogger   *log.Logger
	HeartBeatLogger *log.Logger
	ReceivedLogger  *log.Logger
	InfoLogger      *log.Logger
	IncarLogger     *log.Logger
	AppendLogger    *log.Logger
	CreateLogger    *log.Logger
	GetLogger       *log.Logger
)

func NewVMLog(path string) error {
	file, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("cannot open the log file")
	}
	logFlag := log.Ldate | log.Ltime | log.Lmicroseconds
	JoinLogger = log.New(file, "[JOIN]", logFlag)
	SuspectedLogger = log.New(file, "[Suspected]", logFlag)
	FailedLogger = log.New(file, "[Failed]", logFlag)
	RefutedLogger = log.New(file, "[Refuted]", logFlag)
	DeletedLogger = log.New(file, "[Deleted]", logFlag)
	HeartBeatLogger = log.New(file, "[HeartBeat]", logFlag)
	ReceivedLogger = log.New(file, "[Received]", logFlag)
	InfoLogger = log.New(file, "[Info]", logFlag)
	IncarLogger = log.New(file, "[Incar]", logFlag)
	AppendLogger = log.New(file, "[Append]", logFlag)
	CreateLogger = log.New(file, "[Create]", logFlag)
	GetLogger = log.New(file, "[Get]", logFlag)
	return nil
}

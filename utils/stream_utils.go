package utils

import (
	"fmt"
	"time"
)

type StreamTuple struct {
	Key       string
	Value     string
	MessageID string
	IsEOS     bool // End Of Stream marker
}

type StartTaskArgs struct {
	RSID            string
	StageID         int
	TaskID          int
	OpExe           string
	OpArgs          string
	IsSource        bool
	IsLastStage     bool
	HyDFSOutput     string
	DownstreamTasks []string
	ExactlyOnce     bool
	NumUpstream     int
	HyDFSDedupLog   string
}

type StartTaskReply struct {
	Success      bool
	ErrorMessage string
	PID          int
	LogFileName  string
}

type StartSourceArgs struct {
	HyDFSFileName  string
	InputRate      int
	Stage1RPCAddrs []string
}

type StartSourceReply struct {
	Success      bool
	ErrorMessage string
}

type PushTupleArgs struct {
	DestStageID int
	DestTaskID  int
	Tuples      []StreamTuple
}

type PushTupleReply struct {
	Success      bool
	ErrorMessage string
}

type KillTaskArgs struct {
	PID int
}

type KillTaskReply struct {
	Success      bool
	ErrorMessage string
}

type ReportRateArgs struct {
	StageID      int
	TaskID       int
	TuplesPerSec float64
}

type ReportRateReply struct {
	Success bool
}

type LeaderStartArgs struct {
	RSID        string
	NumStages   int
	NumTasks    int
	OpExes      []string
	OpArgs      []string
	SrcFile     string
	DestFile    string
	ExactlyOnce bool
	Autoscale   bool
	InputRate   int
	LW          int
	HW          int
}

type LeaderStartReply struct {
	Success      bool
	ErrorMessage string
}

type TaskFinishedArgs struct {
	StageID int
	TaskID  int
	Status  string // "success" or "failure"
}

type TaskFinishedReply struct {
	Success bool
}

type UpdateDownstreamArgs struct {
	StageID         int
	DownstreamTasks []string
}

type UpdateDownstreamReply struct {
	Success bool
}

type UpdateUpstreamArgs struct {
	StageID     int
	NumUpstream int
}

type UpdateUpstreamReply struct {
	Success bool
}

type ProcessOrphanArgs struct {
	StageID       int
	TaskID        int
	OrphanLogFile string
}

type ProcessOrphanReply struct {
	Success      bool
	ErrorMessage string
}

func CallHyDFSWrite(fileName string, data []byte, method string, opID string) error {
	// Get replicas
	replicas, err := GetReplica(fileName)
	if err != nil || len(replicas) == 0 {
		return fmt.Errorf("failed to get replicas for %s: %v", fileName, err)
	}

	// Get primary and non-primary
	primaryAddr := replicas[0].Address
	var nonPrimary []string
	for i := 1; i < len(replicas); i++ {
		nonPrimary = append(nonPrimary, replicas[i].Address)
	}

	// Prepare arguments
	args := &WriteFileArgs{
		HyDFSFileName: fileName,
		LocalFile:     data,
		NonPrimary:    nonPrimary,
		OperationID:   opID,
	}
	reply := &WriteFileReply{}

	// Call RPC
	err = CallRPC(primaryAddr, method, args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("%s\n", reply.ErrorMessage)
	}

	return nil
}

func SendTupleWithRetry(addr string, args *PushTupleArgs) {
	for {
		reply := &PushTupleReply{}
		err := CallRPC(addr, "Node.PushTuple", args, reply)
		if err == nil && reply.Success {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

package node

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/constants"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/streamlog"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/utils"
)

type RainStormTask struct {
	Cmd            *exec.Cmd
	Stdin          io.WriteCloser
	Stdout         io.ReadCloser
	ProcessedIDs   map[string]bool
	Lock           sync.Mutex
	SeqNum         int64
	SLog           *streamlog.StreamLoggers
	Counter        int64
	CounterLock    sync.Mutex
	Done           chan struct{}
	DedupHYDFSFile string
	NumUpstream    int
	EOSCount       int
	EOSLock        sync.Mutex
	DownstreamList []string
	DownstreamLock sync.RWMutex
}

type StageInfo struct {
	Tasks map[int]TaskInfo // Key: StageID
}

type TaskInfo struct {
	StageID      int
	TaskID       int
	VMAddr       string
	VMName       string
	PID          int
	OpExe        string
	LogFileName  string
	OriginalArgs *utils.StartTaskArgs // For restart
	DedupFile    string
}

func getTaskKey(stageID int, taskID int) string {
	return fmt.Sprintf("%d:%d", stageID, taskID)
}

func (node *Node) StartRainStorm(args *utils.LeaderStartArgs, reply *utils.LeaderStartReply) error {
	// Init Leader State
	if err := node.resetLeaderState(args); err != nil {
		reply.Success = false
		reply.ErrorMessage = err.Error()
		return nil
	}
	fmt.Printf("[Leader] Received RainStorm Job %s request. Scheduling...\n", args.RSID)

	// Get Alive Workers
	aliveNodes, err := node.getAliveWorkers()
	if err != nil {
		node.LogLock.RLock()
		if node.LeaderLog != nil {
			node.LeaderLog.ErrorLogger.Printf("%v\n", err)
		}
		node.LogLock.RUnlock()
		reply.Success = false
		reply.ErrorMessage = err.Error()
		return nil
	}

	// Round Robin for distributing stages to VMs
	deploymentPlan := node.calculateDeploymentPlan(args.NumStages, args.NumTasks, aliveNodes)

	// Calculate total tasks
	node.FinishLock.Lock()
	node.TotalTasks = args.NumStages * args.NumTasks
	node.FinishedTasks = 0
	node.FinishLock.Unlock()

	// Create Dedup Log files for all tasks for exactly-once
	taskDedupLog := make(map[int]map[int]string)
	var fileWg sync.WaitGroup
	//fmt.Println("[Leader] Pre-creating Dedup Log files...")
	for stageID := 1; stageID <= args.NumStages; stageID++ {
		taskDedupLog[stageID] = make(map[int]string)
		for taskID := 0; taskID < args.NumTasks; taskID++ {
			timestamp := time.Now().UnixNano()
			fName := fmt.Sprintf("dedup_%s_stage%d_task%d_%d", args.RSID, stageID, taskID, timestamp)
			taskDedupLog[stageID][taskID] = fName
			fileWg.Add(1)
			go func(name string) {
				defer fileWg.Done()
				node.createHyDFSFile(name, "Dedup Log")
			}(fName)
		}
	}
	fileWg.Wait()

	// Create Output File
	if err := node.createHyDFSFile(args.DestFile, "Output File"); err != nil {
		reply.Success = false
		reply.ErrorMessage = fmt.Sprintf("Failed to create output file: %v", err)
		return nil
	}

	// Trigger all tasks
	if err := node.startAllTasks(args, deploymentPlan, taskDedupLog); err != nil {
		reply.Success = false
		reply.ErrorMessage = err.Error()
		return nil
	}

	// Trigger Source
	node.triggerSource(args, deploymentPlan)

	if args.Autoscale {
		fmt.Printf("[Leader] Autoscaling ENABLED (Rate=%d, LW=%d, HW=%d)\n", args.InputRate, args.LW, args.HW)
		node.AutoLW = args.LW
		node.AutoHW = args.HW
		go node.monitorAutoscale(args)
	} else {
		fmt.Println("[Leader] Autoscaling DISABLED.")
	}

	reply.Success = true
	return nil
}

func (node *Node) resetLeaderState(args *utils.LeaderStartArgs) error {
	node.LogLock.Lock()
	defer node.LogLock.Unlock()

	if node.LeaderLog != nil {
		node.LeaderLog.Close()
	}
	var err error
	node.LeaderLog, _, err = streamlog.NewStreamLogger(0, 0, "rs_leader")
	if err != nil {
		return fmt.Errorf("Failed to create leader log: %v", err)
	}

	node.LeaderLog.JobLogger.Printf("Starting Job: Stages=%d, Tasks=%d, Src=%s, EO=%t\n",
		args.NumStages, args.NumTasks, args.SrcFile, args.ExactlyOnce)

	node.TaskMapLock.Lock()
	node.GlobalTaskMap = make(map[int]*StageInfo)
	node.TaskMapLock.Unlock()

	node.TaskRatesLock.Lock()
	node.TaskRates = make(map[int]map[int]float64)
	node.TaskRatesLock.Unlock()
	node.AutoscaleStopChan = make(chan struct{})

	return nil
}

func (node *Node) getAliveWorkers() ([]string, error) {
	aliveNodes := []string{}
	selfIP, _ := utils.VMNameToIP("VM1")

	memberlist := node.memberlist.getMemberList()
	for _, m := range memberlist {
		if m.State == constants.Alive {
			rpcAddr, _ := utils.Gossip2RPCAddr(m.Address)
			memberIP := strings.Split(rpcAddr, ":")[0]
			if memberIP == selfIP {
				continue
			}
			aliveNodes = append(aliveNodes, rpcAddr)
		}
	}
	sort.Strings(aliveNodes)

	if len(aliveNodes) == 0 {
		return nil, fmt.Errorf("No alive workers found")
	}
	return aliveNodes, nil
}

func (node *Node) calculateDeploymentPlan(numStages, numTasks int, aliveNodes []string) map[int][]string {
	plan := make(map[int][]string)
	globalTaskIdx := 0
	for stageID := 1; stageID <= numStages; stageID++ {
		plan[stageID] = make([]string, numTasks)
		for taskID := 0; taskID < numTasks; taskID++ {
			nodeIdx := globalTaskIdx % len(aliveNodes)
			plan[stageID][taskID] = aliveNodes[nodeIdx]
			globalTaskIdx++
		}
	}
	return plan
}

func (node *Node) startAllTasks(args *utils.LeaderStartArgs, plan map[int][]string, taskDedupLogs map[int]map[int]string) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0

	for stageID := 1; stageID <= args.NumStages; stageID++ {
		for taskID := 0; taskID < args.NumTasks; taskID++ {
			targetAddr := plan[stageID][taskID]

			vmName := "Unknown"
			if name, ok := utils.GetVMName(targetAddr); ok {
				vmName = name
			}
			displayHost := fmt.Sprintf("%s (%s)", vmName, targetAddr)

			var downstream []string
			if stageID < args.NumStages {
				downstream = plan[stageID+1]
			}

			prevStageTasks := 0
			if stageID == 1 {
				prevStageTasks = 1 // Source only one task
			} else {
				prevStageTasks = args.NumTasks
			}
			HyDFSDedupLog := taskDedupLogs[stageID][taskID]

			taskArgs := &utils.StartTaskArgs{
				RSID:            args.RSID,
				StageID:         stageID,
				TaskID:          taskID,
				OpExe:           args.OpExes[stageID-1],
				OpArgs:          args.OpArgs[stageID-1],
				IsLastStage:     (stageID == args.NumStages),
				HyDFSOutput:     args.DestFile,
				ExactlyOnce:     args.ExactlyOnce,
				DownstreamTasks: downstream,
				NumUpstream:     prevStageTasks,
				HyDFSDedupLog:   HyDFSDedupLog,
			}

			wg.Add(1)
			go func(addr string, tArgs *utils.StartTaskArgs) {
				defer wg.Done()
				tReply := &utils.StartTaskReply{}
				err := utils.CallRPC(addr, "Node.StartTask", tArgs, tReply)

				if err == nil && tReply.Success {
					mu.Lock()
					successCount++
					node.TaskMapLock.Lock()
					if _, ok := node.GlobalTaskMap[tArgs.StageID]; !ok {
						node.GlobalTaskMap[tArgs.StageID] = &StageInfo{
							Tasks: make(map[int]TaskInfo),
						}
					}
					node.GlobalTaskMap[tArgs.StageID].Tasks[tArgs.TaskID] = TaskInfo{
						StageID:      tArgs.StageID,
						TaskID:       tArgs.TaskID,
						VMAddr:       addr,
						VMName:       vmName,
						PID:          tReply.PID,
						OpExe:        tArgs.OpExe,
						LogFileName:  tReply.LogFileName,
						OriginalArgs: tArgs,
						DedupFile:    tArgs.HyDFSDedupLog,
					}
					node.TaskMapLock.Unlock()
					mu.Unlock()

					node.LogLock.RLock()
					if node.LeaderLog != nil {
						node.LeaderLog.TaskLogger.Printf("[Start] Stage %d Task %d on %s (PID %d) | Exe: %s | Log: %s\n",
							tArgs.StageID, tArgs.TaskID, displayHost, tReply.PID, tArgs.OpExe, tReply.LogFileName)
					}
					node.LogLock.RUnlock()
				} else {
					node.LogLock.RLock()
					if node.LeaderLog != nil {
						node.LeaderLog.ErrorLogger.Printf("Failed to start task on %s: %v\n", addr, err)
					}
					node.LogLock.RUnlock()
				}
			}(targetAddr, taskArgs)
		}
	}
	wg.Wait()

	if successCount < args.NumStages*args.NumTasks {
		return fmt.Errorf("Some tasks failed to start")
	}
	return nil
}

func (node *Node) createHyDFSFile(fileName string, desc string) error {

	opID := fmt.Sprintf("init-%s-%d", fileName, time.Now().UnixNano())
	//fmt.Printf("[Leader] Creating %s: %s \n", desc, fileName)
	err := utils.CallHyDFSWrite(fileName, []byte{}, "Node.CreateFile", opID)

	if err != nil {
		fmt.Printf("[Leader]: %s creation returned error: %v\n", desc, err)
	} else {
		node.LogLock.RLock()
		if node.LeaderLog != nil {
			node.LeaderLog.DedupLogger.Printf("%s %s created successfully.\n", desc, fileName)
		}
		node.LogLock.RUnlock()
		//fmt.Printf("[Leader] %s %s created successfully.\n", desc, fileName)
	}
	return nil
}

func (node *Node) triggerSource(args *utils.LeaderStartArgs, plan map[int][]string) {
	sourceArgs := &utils.StartSourceArgs{
		HyDFSFileName:  args.SrcFile,
		InputRate:      args.InputRate,
		Stage1RPCAddrs: plan[1],
	}
	node.StartSource(sourceArgs, &utils.StartSourceReply{})

	node.LogLock.RLock()
	if node.LeaderLog != nil {
		node.LeaderLog.InfoLogger.Printf("Source Started. Job Running.\n")
	}
	node.LogLock.RUnlock()
}

// MP4 Start a RainStorm Task (operator)
func (node *Node) StartTask(args *utils.StartTaskArgs, reply *utils.StartTaskReply) error {
	// Init logger for each task
	sLog, logName, err := streamlog.NewStreamLogger(args.StageID, args.TaskID, "")
	if err != nil {
		reply.Success = false
		reply.ErrorMessage = "Failed to create logger"
		return nil
	}
	sLog.TaskLogger.Printf("Task Started. Exe: %s, Args: %s\n", args.OpExe, args.OpArgs)
	fmt.Printf("Starting Task: Stage %d, Task %d, Exe %s\n", args.StageID, args.TaskID, args.OpExe)

	// Load existing dedup log for exactly-once
	dedupFileName := args.HyDFSDedupLog
	processedIDs, replayData := node.loadDedupLog(dedupFileName, sLog)

	// Start the operator process
	cmd := exec.Command("./" + args.OpExe)
	if args.OpArgs != "" {
		argParts := strings.Fields(args.OpArgs)
		cmd.Args = append(cmd.Args, argParts...)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		sLog.ErrorLogger.Printf("Failed to create StdinPipe: %v\n", err)
		sLog.Close()
		reply.Success = false
		reply.ErrorMessage = "Failed to create StdinPipe: " + err.Error()
		return nil
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		sLog.ErrorLogger.Printf("Failed to create StdoutPipe: %v\n", err)
		sLog.Close()
		reply.Success = false
		reply.ErrorMessage = "Failed to create StdoutPipe: " + err.Error()
		return nil
	}

	if err := cmd.Start(); err != nil {
		sLog.ErrorLogger.Printf("Failed to start command: %v\n", err)
		sLog.Close()
		reply.Success = false
		reply.ErrorMessage = "Failed to start command: " + err.Error()
		return nil
	}

	newTask := &RainStormTask{
		Cmd:            cmd,          // for kil task
		Stdin:          stdin,        // for PushTuple
		Stdout:         stdout,       // for handleTaskOutput
		ProcessedIDs:   processedIDs, // exactly-once
		SeqNum:         0,
		SLog:           sLog,
		Counter:        0,
		Done:           make(chan struct{}),
		DedupHYDFSFile: dedupFileName,
		NumUpstream:    args.NumUpstream,
		EOSCount:       0,
		DownstreamList: args.DownstreamTasks,
	}

	// Register the task (since it already created the dedup map)
	key := getTaskKey(args.StageID, args.TaskID)
	node.rsTaskMutex.Lock()
	if node.rsTasks == nil {
		node.rsTasks = make(map[string]*RainStormTask)
	}
	if node.TaskPidMap == nil {
		node.TaskPidMap = make(map[int]string)
	}
	node.rsTasks[key] = newTask
	node.TaskPidMap[cmd.Process.Pid] = key
	node.rsTaskMutex.Unlock()

	// Replay existing data for exactly-once
	if len(replayData) > 0 {
		go node.replayLogData(newTask, replayData, false, "Self Dedup Log")
	}

	go node.monitorTaskRate(newTask, args.StageID, args.TaskID)
	go node.handleTaskOutput(newTask, args)

	reply.Success = true
	reply.PID = cmd.Process.Pid
	reply.LogFileName = logName
	return nil
}

func (node *Node) loadDedupLog(fileName string, sLog *streamlog.StreamLoggers) (map[string]bool, []byte) {
	processedIDs := make(map[string]bool)
	var fileData []byte
	replicas, err := utils.GetReplica(fileName)
	if err == nil && len(replicas) > 0 {
		getArgs := &utils.GetFileArgs{HyDFSFileName: fileName}
		getReply := &utils.GetFileReply{}
		err = utils.CallRPC(replicas[0].Address, "Node.GetFile", getArgs, getReply)

		if err == nil && getReply.Success {
			fileData = getReply.FileData
			scanner := bufio.NewScanner(bytes.NewReader(fileData))
			count := 0
			for scanner.Scan() {
				line := scanner.Text()
				parts := strings.SplitN(line, "\t", 2)
				if len(parts) >= 1 {
					id := strings.TrimSpace(parts[0])
					if id != "" {
						processedIDs[id] = true
						count++
					}
				}
			}
			sLog.InfoLogger.Printf("[Recovery] Recovered %d IDs from %s\n", count, fileName)
			return processedIDs, fileData
		}
	}
	sLog.InfoLogger.Printf("[Recovery] No existing log found for %s, starting fresh.\n", fileName)
	return processedIDs, nil
}

// MP4 handle task output (last stage need to write to HYDFS, others push to downstream)
func (node *Node) handleTaskOutput(task *RainStormTask, args *utils.StartTaskArgs) {
	defer close(task.Done)
	scanner := bufio.NewScanner(task.Stdout)
	var tupleWg sync.WaitGroup
	for scanner.Scan() {
		line := scanner.Text()
		task.SLog.OutputLogger.Printf("%s\n", line)
		task.Lock.Lock()
		task.SeqNum++
		currentSeq := task.SeqNum
		task.Lock.Unlock()

		// Unique ID: "StageID-TaskID-SeqNum"
		outMsgID := fmt.Sprintf("%d-%d-%d", args.StageID, args.TaskID, currentSeq)

		if args.IsLastStage {
			fmt.Printf("[Output Stage%d-Task%d]: %s\n", args.StageID, args.TaskID, line)
			opID := fmt.Sprintf("append-%s-%d", outMsgID, time.Now().UnixNano())
			data := []byte(line + "\n")
			err := utils.CallHyDFSWrite(args.HyDFSOutput, data, "Node.AppendFile", opID)
			if err != nil {
				task.SLog.ErrorLogger.Printf("AppendFile failed: %v\n", err)
			}
		} else {
			var key, value string
			if idx := strings.Index(line, "\t"); idx != -1 {
				// Application 1, having tab
				key = line[:idx]
				value = line[idx+1:]
			} else {
				// Application 2, no aggregate
				key = line
				value = line
			}
			tupleWg.Add(1)
			go node.sendTupleWithDynamicRouting(task, args.StageID+1, key, value, outMsgID, &tupleWg)
		}
	}

	// Wait for all tuples to be acked before proceeding to EOS
	if !args.IsLastStage {
		task.SLog.InfoLogger.Println("Waiting for pending tuples to be acked...")
		tupleWg.Wait()
	}

	task.EOSLock.Lock()
	isSuccess := (task.EOSCount >= task.NumUpstream)
	task.EOSLock.Unlock()

	status := "Failure"
	if isSuccess {
		status = "Success"
		task.SLog.InfoLogger.Printf("Task finished successfully (EOS).\n")
		// Send EOS to all downstream tasks
		if !args.IsLastStage {
			eosTuple := utils.StreamTuple{IsEOS: true, MessageID: "EOS"}
			var eosWg sync.WaitGroup
			task.DownstreamLock.RLock()
			finalDownstream := make([]string, len(task.DownstreamList))
			copy(finalDownstream, task.DownstreamList)
			task.DownstreamLock.RUnlock()
			for i, addr := range finalDownstream {
				eosArgs := &utils.PushTupleArgs{
					DestStageID: args.StageID + 1,
					DestTaskID:  i,
					Tuples:      []utils.StreamTuple{eosTuple},
				}
				eosWg.Add(1)
				go func(target string, pArgs *utils.PushTupleArgs) {
					defer eosWg.Done()
					utils.SendTupleWithRetry(target, pArgs)
				}(addr, eosArgs)
			}
			eosWg.Wait()
		}
	} else {
		task.SLog.ErrorLogger.Printf("Task process exited unexpectedly (Crash/Kill).\n")
	}

	// Report to Leader: either success or failure
	finArgs := &utils.TaskFinishedArgs{
		StageID: args.StageID,
		TaskID:  args.TaskID,
		Status:  status,
	}
	leaderIP, _ := utils.VMNameToIP("VM1")
	leaderRPC := leaderIP + ":" + constants.RPCPort

	go func() {
		reply := &utils.TaskFinishedReply{}
		err := utils.CallRPC(leaderRPC, "Node.TaskFinished", finArgs, reply)
		if err != nil {
			task.SLog.ErrorLogger.Printf("Failed to report TaskFinished to Leader: %v\n", err)
		}
	}()
}

// MP4: Ensure at least once for autoscale
func (node *Node) sendTupleWithDynamicRouting(task *RainStormTask, nextStageID int, key, value, msgID string, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		task.DownstreamLock.RLock()
		currentList := make([]string, len(task.DownstreamList))
		copy(currentList, task.DownstreamList)
		task.DownstreamLock.RUnlock()
		if len(currentList) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		hashVal := utils.HashFunc(key)
		targetIndex := int(hashVal) % len(currentList)
		targetAddr := currentList[targetIndex]
		pushArgs := &utils.PushTupleArgs{
			DestStageID: nextStageID,
			DestTaskID:  targetIndex,
			Tuples: []utils.StreamTuple{
				{
					Key:       key,
					Value:     value,
					MessageID: msgID,
				},
			},
		}
		reply := &utils.PushTupleReply{}
		err := utils.CallRPC(targetAddr, "Node.PushTuple", pushArgs, reply)
		if err == nil && reply.Success {
			break
		}
		// Retry after new downstream list
		time.Sleep(200 * time.Millisecond)
	}
}

// MP4: get the key and tuple from upstream tasks, check duplicate and push to next operator
func (node *Node) PushTuple(args *utils.PushTupleArgs, reply *utils.PushTupleReply) error {
	key := getTaskKey(args.DestStageID, args.DestTaskID)
	node.rsTaskMutex.Lock()
	task, exists := node.rsTasks[key]
	node.rsTaskMutex.Unlock()

	if !exists {
		reply.Success = false
		reply.ErrorMessage = fmt.Sprintf("Task %s not found on this node", key)
		return nil
	}

	task.Lock.Lock()
	defer task.Lock.Unlock()

	for _, tuple := range args.Tuples {
		if tuple.IsEOS {
			task.EOSLock.Lock()
			task.EOSCount++
			isDone := (task.EOSCount >= task.NumUpstream)
			task.EOSLock.Unlock()

			if isDone {
				task.SLog.InfoLogger.Printf("Received all EOS. Closing Stdin.\n")
				task.Stdin.Close()
			}
			continue
		}

		// exact once processing
		if task.ProcessedIDs[tuple.MessageID] {
			task.SLog.DuplicateLogger.Printf("Rejected tuple with ID: %s\n", tuple.MessageID)
			continue
		}

		if task.DedupHYDFSFile == "" {
			reply.Success = false
			reply.ErrorMessage = "Dedup Log filename missing"
			return nil
		}
		logContent := fmt.Sprintf("%s\t%s\n", tuple.MessageID, tuple.Value)
		opID := fmt.Sprintf("dedup-%d-%s", time.Now().UnixNano(), tuple.MessageID)
		err := utils.CallHyDFSWrite(task.DedupHYDFSFile, []byte(logContent), "Node.AppendFile", opID)

		if err != nil {
			task.SLog.ErrorLogger.Printf("Failed to write dedup log for %s: %v\n", tuple.MessageID, err)
			reply.Success = false
			reply.ErrorMessage = "Failed to persist dedup log: " + err.Error()
			return nil
		}
		//connect to next task stdin
		_, err = io.WriteString(task.Stdin, tuple.Value+"\n")
		if err != nil {
			reply.Success = false
			reply.ErrorMessage = "Failed to write to operator stdin"
			return nil
		}
		// count for rate monitoring
		task.CounterLock.Lock()
		task.Counter++
		task.CounterLock.Unlock()
		task.ProcessedIDs[tuple.MessageID] = true
	}

	reply.Success = true
	return nil
}

// MP4 Source Stream: Leader load the HYDFS and push to stage 1 workers
func (node *Node) StartSource(args *utils.StartSourceArgs, reply *utils.StartSourceReply) error {
	go func() {
		fmt.Printf("[Source] Starting stream file: %s, Rate: %d\n", args.HyDFSFileName, args.InputRate)
		// get HYDFS from any replica, if primary is down, go to next replica
		getArgs := &utils.GetFileArgs{HyDFSFileName: args.HyDFSFileName}
		var fileData []byte
		downloadSuccess := false

		replicas, err := utils.GetReplica(args.HyDFSFileName)
		if err != nil {
			fmt.Printf("[Source] Error: Failed to get replica info: %v\n", err)
			return
		}

		for _, replica := range replicas {
			getReply := &utils.GetFileReply{}
			fmt.Printf("[Source] Trying to download from %s...\n", replica.Address)
			err := utils.CallRPC(replica.Address, "Node.GetFile", getArgs, getReply)
			if err == nil && getReply.Success {
				fileData = getReply.FileData
				downloadSuccess = true
				break
			}
		}

		if !downloadSuccess {
			fmt.Printf("[Source] Critical Error: Failed to download %s from ALL replicas.\n", args.HyDFSFileName)
			return
		}

		scanner := bufio.NewScanner(bytes.NewReader(fileData))

		intervalNs := float64(time.Second) / float64(args.InputRate)
		intervalDuration := time.Duration(intervalNs)
		ticker := time.NewTicker(intervalDuration)
		defer ticker.Stop()

		seq := 0
		var tupleWg sync.WaitGroup
		for scanner.Scan() {
			<-ticker.C
			line := scanner.Text()
			seq++
			// exact one with leader and sequence number for source stage
			msgID := fmt.Sprintf("src-%s-%d", node.vmName, seq)
			tupleWg.Add(1)
			go node.sendSourceTuple(1, line, msgID, seq, &tupleWg)
		}
		fmt.Println("[Source] Streaming finished. Sending EOS...")
		tupleWg.Wait()
		if node.AutoscaleStopChan != nil {
			func() {
				defer func() { recover() }()
				close(node.AutoscaleStopChan)
			}()
		}
		time.Sleep(100 * time.Millisecond) // wait for deactivating autoscale
		currentStage1 := node.getSortedStageWorkers(1)
		var eosWg sync.WaitGroup
		eosTuple := utils.StreamTuple{IsEOS: true, MessageID: "EOS"}
		for i, addr := range currentStage1 {
			pArgs := &utils.PushTupleArgs{
				DestStageID: 1,
				DestTaskID:  i,
				Tuples:      []utils.StreamTuple{eosTuple},
			}
			eosWg.Add(1)
			go func(target string, pArgs *utils.PushTupleArgs) {
				defer eosWg.Done()
				utils.SendTupleWithRetry(target, pArgs)
			}(addr, pArgs)
		}
		eosWg.Wait()
		node.LogLock.RLock()
		if node.LeaderLog != nil {
			node.LeaderLog.JobLogger.Printf("End of Entire Run (Source finished reading input)\n")
		}
		node.LogLock.RUnlock()
	}()

	reply.Success = true
	return nil
}

func (node *Node) ListTasks(args *struct{}, reply *string) error {
	node.TaskMapLock.RLock()
	defer node.TaskMapLock.RUnlock()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%-5s | %-4s | %-25s | %-7s | %-15s | %s\n",
		"Stage", "Task", "VM Address", "PID", "Op Exe", "Log File"))
	sb.WriteString(strings.Repeat("-", 100) + "\n")

	var stages []int
	for sID := range node.GlobalTaskMap {
		stages = append(stages, sID)
	}
	sort.Ints(stages)
	for _, sID := range stages {
		stageInfo := node.GlobalTaskMap[sID]
		// Sort by TaskID
		var tasks []int
		for tID := range stageInfo.Tasks {
			tasks = append(tasks, tID)
		}
		sort.Ints(tasks)

		for _, tID := range tasks {
			info := stageInfo.Tasks[tID]
			vmName, found := utils.GetVMName(info.VMAddr)
			displayVM := info.VMAddr
			if found {
				displayVM = fmt.Sprintf("%s(%s)", vmName, info.VMAddr)
			}
			sb.WriteString(fmt.Sprintf("%5d | %4d | %-25s | %7d | %-15s | %s\n",
				info.StageID, info.TaskID, displayVM, info.PID, info.OpExe, info.LogFileName))
		}
	}
	*reply = sb.String()
	return nil
}

func (node *Node) KillTask(args *utils.KillTaskArgs, reply *utils.KillTaskReply) error {
	process, err := os.FindProcess(args.PID)
	if err != nil {
		reply.Success = false
		reply.ErrorMessage = fmt.Sprintf("FindProcess failed: %v", err)
		return nil
	}

	err = process.Kill()
	if err != nil {
		reply.Success = false
		reply.ErrorMessage = fmt.Sprintf("Failed to kill PID %d: %v", args.PID, err)
		return nil
	}

	node.rsTaskMutex.Lock()
	defer node.rsTaskMutex.Unlock()
	if targetKey, exists := node.TaskPidMap[args.PID]; exists {
		if _, taskExists := node.rsTasks[targetKey]; taskExists {
			delete(node.rsTasks, targetKey)
			fmt.Printf("[KillTask] Removed task %s from rsTasks map\n", targetKey)
		}
		delete(node.TaskPidMap, args.PID)

	} else {
		fmt.Printf("[KillTask] Warning: PID %d not found in TaskPidMap\n", args.PID)
	}
	reply.Success = true
	return nil
}

func (node *Node) TaskFinished(args *utils.TaskFinishedArgs, reply *utils.TaskFinishedReply) error {
	if args.Status == "Success" {
		node.TaskMapLock.RLock()
		displayHost := "Unknown"
		if stageInfo, ok := node.GlobalTaskMap[args.StageID]; ok {
			if info, ok := stageInfo.Tasks[args.TaskID]; ok {
				displayHost = fmt.Sprintf("%s (%s)", info.VMName, info.VMAddr)
			}
		}
		node.TaskMapLock.RUnlock()
		node.LogLock.RLock()
		if node.LeaderLog != nil {
			node.LeaderLog.TaskLogger.Printf("[End] Stage %d Task %d on %s (Success)\n", args.StageID, args.TaskID, displayHost)
		}
		node.LogLock.RUnlock()
		node.FinishLock.Lock()
		node.FinishedTasks++
		currentFinished := node.FinishedTasks
		total := node.TotalTasks
		node.FinishLock.Unlock()
		node.LogLock.RLock()
		if node.LeaderLog != nil {
			node.LeaderLog.InfoLogger.Printf("Task Completed Successfully: Stage %d Task %d\n", args.StageID, args.TaskID)
		}
		// Actual completion check
		if currentFinished >= total {
			if node.LeaderLog != nil {
				node.LeaderLog.JobLogger.Printf("RAINSTORM JOB COMPLETED: All %d tasks finished successfully.\n", total)
				fmt.Println("[Leader] RAINSTORM JOB COMPLETED. All tasks finished.")
			}
		}
		node.LogLock.RUnlock()
		reply.Success = true
		return nil
	}
	// Leader kill because of Autoscale
	node.TaskMapLock.Lock()
	var taskInfo TaskInfo
	var exists bool
	if stageInfo, ok := node.GlobalTaskMap[args.StageID]; ok {
		taskInfo, exists = stageInfo.Tasks[args.TaskID]
	}
	node.TaskMapLock.Unlock()
	if !exists {
		reply.Success = true
		return nil
	}
	// Restart task on failure
	fmt.Printf("[Leader] Detected failure for Stage %d Task %d. Initiating restart...\n", args.StageID, args.TaskID)
	// Check Task Info
	node.LogLock.RLock()
	if node.LeaderLog != nil {
		node.LeaderLog.FailLogger.Printf("Task Failed/Crashed: Stage %d Task %d (Status: %s) -> Restarting\n",
			args.StageID, args.TaskID, args.Status)
		displayHost := fmt.Sprintf("%s (%s)", taskInfo.VMName, taskInfo.VMAddr)
		node.LeaderLog.TaskLogger.Printf("[Restart] Stage %d Task %d on %s (Old PID: %d, Exe: %s)\n",
			taskInfo.StageID, taskInfo.TaskID, displayHost, taskInfo.PID, taskInfo.OpExe)
	}
	node.LogLock.RUnlock()
	go func(info TaskInfo) {
		// Restart same VM, assume VM is alive
		targetAddr := info.VMAddr
		if info.OriginalArgs == nil {
			fmt.Printf("[Leader] Critical: Missing original args for restart\n")
			return
		}

		restartReply := &utils.StartTaskReply{}
		err := utils.CallRPC(targetAddr, "Node.StartTask", info.OriginalArgs, restartReply)

		if err == nil && restartReply.Success {
			// Restart Success
			node.TaskMapLock.Lock()
			if stageInfo, ok := node.GlobalTaskMap[args.StageID]; ok {
				if entry, ok := stageInfo.Tasks[args.TaskID]; ok {
					entry.PID = restartReply.PID
					entry.LogFileName = restartReply.LogFileName
					stageInfo.Tasks[args.TaskID] = entry // Update
				}
			}
			node.TaskMapLock.Unlock()

			// Write to new task log
			node.LogLock.RLock()
			if node.LeaderLog != nil {
				node.LeaderLog.InfoLogger.Printf("Task Restarted Successfully: Stage %d Task %d on %s (New PID %d)\n",
					info.StageID, info.TaskID, targetAddr, restartReply.PID)
			}
			node.LogLock.RUnlock()

			fmt.Printf("[Leader] Stage %d Task %d restarted successfully on %s\n", info.StageID, info.TaskID, targetAddr)

		} else {
			fmt.Printf("[Leader] Failed to restart task %d-%d: %v\n", info.StageID, info.TaskID, err)
		}
	}(taskInfo)

	reply.Success = true
	return nil
}

func (node *Node) ProcessOrphanLog(args *utils.ProcessOrphanArgs, reply *utils.ProcessOrphanReply) error {

	// Find Survivor Task (Task 0 of the same stage)
	key := getTaskKey(args.StageID, args.TaskID)
	node.rsTaskMutex.Lock()
	task, exists := node.rsTasks[key]
	node.rsTaskMutex.Unlock()

	if !exists {
		reply.Success = false
		reply.ErrorMessage = fmt.Sprintf("Critical: Survivor task %d not found on node %s. Cannot replay log.", args.TaskID, node.vmName)
		return nil
	}

	fmt.Printf("[Recovery] Task %d taking over orphan log: %s\n", args.TaskID, args.OrphanLogFile)
	// Download the orphan log file from HyDFS
	getArgs := &utils.GetFileArgs{HyDFSFileName: args.OrphanLogFile}
	var fileData []byte

	replicas, err := utils.GetReplica(args.OrphanLogFile)
	if err == nil {
		for _, r := range replicas {
			getReply := &utils.GetFileReply{}
			if utils.CallRPC(r.Address, "Node.GetFile", getArgs, getReply) == nil && getReply.Success {
				fileData = getReply.FileData
				break
			}
		}
	}
	if len(fileData) == 0 {
		fmt.Printf("[Recovery] Warning: Orphan log %s is empty or download failed.\n", args.OrphanLogFile)
		reply.Success = true
		return nil
	}

	go node.replayLogData(task, fileData, true, fmt.Sprintf("Orphan Log %s", args.OrphanLogFile))

	reply.Success = true
	return nil
}

func (node *Node) replayLogData(task *RainStormTask, data []byte, enableDedupCheck bool, sourceDesc string) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	count := 0

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) == 2 {
			msgID := parts[0]
			val := parts[1]
			task.Lock.Lock()
			shouldWrite := true
			if enableDedupCheck {
				// Test3 scaledown orphan log replay
				if task.ProcessedIDs[msgID] {
					shouldWrite = false
				} else {
					task.ProcessedIDs[msgID] = true
				}
			} else {
				// Test2 leader restart replay all
				shouldWrite = true
			}

			if shouldWrite {
				if _, err := io.WriteString(task.Stdin, val+"\n"); err == nil {
					count++
				}
			}
			task.Lock.Unlock()
		}
	}

	logMsg := fmt.Sprintf("Replayed %d tuples from %s (DedupCheck=%t)\n", count, sourceDesc, enableDedupCheck)
	fmt.Print(logMsg)
	if task.SLog != nil {
		task.SLog.ReplayLogger.Print(logMsg)
	}
}

func (node *Node) sendSourceTuple(stageID int, line string, msgID string, seq int, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		workers := node.getSortedStageWorkers(stageID)
		numWorkers := len(workers)

		if numWorkers == 0 {
			fmt.Println("[Source] Warning: No Stage 1 workers available, retrying...")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		workerIdx := seq % numWorkers
		targetAddr := workers[workerIdx]
		pushArgs := &utils.PushTupleArgs{
			DestStageID: stageID,
			DestTaskID:  workerIdx,
			Tuples: []utils.StreamTuple{
				{
					Key:       line,
					Value:     line,
					MessageID: msgID,
				},
			},
		}
		reply := &utils.PushTupleReply{}
		err := utils.CallRPC(targetAddr, "Node.PushTuple", pushArgs, reply)
		if err == nil && reply.Success {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}

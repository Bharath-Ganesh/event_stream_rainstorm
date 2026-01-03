package node

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/constants"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/utils"
)

// Call it when scale up or down
func (node *Node) UpdateDownstream(args *utils.UpdateDownstreamArgs, reply *utils.UpdateDownstreamReply) error {
	node.rsTaskMutex.Lock()
	defer node.rsTaskMutex.Unlock()

	updatedCount := 0
	for key, task := range node.rsTasks {
		var currentStageID, currentTaskID int
		n, err := fmt.Sscanf(key, "%d:%d", &currentStageID, &currentTaskID)
		if err != nil || n != 2 {
			continue
		}
		if currentStageID == args.StageID {
			task.DownstreamLock.Lock()
			task.DownstreamList = args.DownstreamTasks
			task.DownstreamLock.Unlock()
			updatedCount++
		}
	}
	if updatedCount > 0 {
		fmt.Printf("[Node] Updated downstream for %d tasks in Stage %d. New count: %d\n",
			updatedCount, args.StageID, len(args.DownstreamTasks))
	}
	reply.Success = true
	return nil
}

func (node *Node) monitorTaskRate(task *RainStormTask, stageID, taskID int) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Get Leader IP
	leaderIP, _ := utils.VMNameToIP("VM1")
	leaderRPC := leaderIP + ":" + constants.RPCPort

	for {
		select {
		case <-task.Done:
			return
		case <-ticker.C:
			task.CounterLock.Lock()
			count := task.Counter
			task.Counter = 0
			task.CounterLock.Unlock()
			// Report back to leader for autoscale
			// Record input rate every second
			task.SLog.RateLogger.Printf("%d tuples/sec\n", count)
			rateArgs := &utils.ReportRateArgs{
				StageID:      stageID,
				TaskID:       taskID,
				TuplesPerSec: float64(count),
			}
			go utils.CallRPC(leaderRPC, "Node.UpdateRate", rateArgs, &utils.ReportRateReply{})
		}
	}
}

// MP4 leader record the rate from tasks, easier for autoscale
func (node *Node) UpdateRate(args *utils.ReportRateArgs, reply *utils.ReportRateReply) error {
	node.LogLock.RLock()
	if node.LeaderLog != nil {
		node.LeaderLog.RateLogger.Printf("Stage %d Task %d: %.2f tuples/sec\n",
			args.StageID, args.TaskID, args.TuplesPerSec)
	}
	node.LogLock.RUnlock()
	node.TaskMapLock.RLock()
	// If it is already gone in the global map
	// No need to update rate (zombie report)
	isAlive := false
	if stageInfo, ok := node.GlobalTaskMap[args.StageID]; ok {
		if _, ok := stageInfo.Tasks[args.TaskID]; ok {
			isAlive = true
		}
	}
	node.TaskMapLock.RUnlock()

	if !isAlive {
		reply.Success = true
		return nil
	}
	// Save to map for autoscale calculation
	node.TaskRatesLock.Lock()
	if _, ok := node.TaskRates[args.StageID]; !ok {
		node.TaskRates[args.StageID] = make(map[int]float64)
	}
	node.TaskRates[args.StageID][args.TaskID] = args.TuplesPerSec
	node.TaskRatesLock.Unlock()
	reply.Success = true
	return nil
}

func (node *Node) monitorAutoscale(jobArgs *utils.LeaderStartArgs) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	fmt.Println("[Autoscaler] Monitoring started.")

	for {
		select {
		case <-node.AutoscaleStopChan:
			fmt.Println("[Autoscale] Monitoring stopped.")
			return
		case <-ticker.C:
			if time.Now().Before(node.ScaleCooldown) {
				continue
			}
			for stage := 1; stage <= jobArgs.NumStages; stage++ {
				var totalRate float64
				var count int
				node.TaskRatesLock.RLock()
				if stageRates, ok := node.TaskRates[stage]; ok {
					for _, rate := range stageRates {
						totalRate += rate
						count++
					}
				}
				node.TaskRatesLock.RUnlock()
				realTaskCount := node.getStageTaskCount(stage)

				if count == 0 {
					continue
				}
				avgRate := totalRate / float64(count)
				if avgRate > float64(node.AutoHW) {
					node.LeaderLog.AutoScaleLogger.Printf("[UP] Stage %d Avg Rate %.2f > HW %d. Triggering Scale UP.", stage, avgRate, node.AutoHW)
					node.scaleUp(jobArgs, stage)
					break
				} else if avgRate < float64(node.AutoLW) {
					// Use global map as gold
					if realTaskCount > 1 {
						node.LeaderLog.AutoScaleLogger.Printf("[DOWN] Stage %d Avg Rate %.2f < LW %d. Triggering Scale DOWN.", stage, avgRate, node.AutoLW)
						node.scaleDown(jobArgs, stage)
						break
					}
				}
			}
		}
	}
}

func (node *Node) scaleUp(args *utils.LeaderStartArgs, stageID int) {
	node.ScaleCooldown = time.Now().Add(1 * time.Second)

	// Find Max Task ID
	node.TaskMapLock.RLock()
	maxID := -1
	if stageInfo, ok := node.GlobalTaskMap[stageID]; ok {
		for tID := range stageInfo.Tasks {
			if tID > maxID {
				maxID = tID
			}
		}
	}
	node.TaskMapLock.RUnlock()
	newTaskID := maxID + 1

	// Find Best Machine (Least Loaded)
	aliveNodes, err := node.getAliveWorkers()
	if err != nil || len(aliveNodes) == 0 {
		fmt.Println("[Autoscale] No workers available.")
		return
	}
	targetAddr := node.getLeastLoadedWorker(aliveNodes)
	vmName := "Unknown"
	if name, ok := utils.GetVMName(targetAddr); ok {
		vmName = name
	}
	displayHost := fmt.Sprintf("%s (%s)", vmName, targetAddr)
	// Create Dedup Log for scale up task
	timestamp := time.Now().UnixNano()
	dedupFileName := fmt.Sprintf("dedup_%s_stage%d_task%d_%d", args.RSID, stageID, newTaskID, timestamp)
	err = node.createHyDFSFile(dedupFileName, "[ScaleUP] Dedup Log")
	if err != nil {
		fmt.Printf("[Autoscale] Error creating dedup file for new task: %v\n", err)
		return
	}
	fmt.Printf("[Autoscale] >>> Scaling Up >>> Starting Stage %d Task %d on %s (Least Loaded)\n", stageID, newTaskID, displayHost)
	opExe := args.OpExes[stageID-1]
	opArg := args.OpArgs[stageID-1]
	var downstream []string = nil
	if stageID < args.NumStages {
		downstream = node.getSortedStageWorkers(stageID + 1)
	}

	prevStageTasks := 0
	if stageID == 1 {
		prevStageTasks = 1
	} else {
		prevStageTasks = node.getStageTaskCount(stageID - 1)
	}

	taskArgs := &utils.StartTaskArgs{
		RSID:            args.RSID,
		StageID:         stageID,
		TaskID:          newTaskID,
		OpExe:           opExe,
		OpArgs:          opArg,
		IsLastStage:     (stageID == args.NumStages),
		HyDFSOutput:     args.DestFile,
		ExactlyOnce:     args.ExactlyOnce,
		DownstreamTasks: downstream,
		NumUpstream:     prevStageTasks,
		HyDFSDedupLog:   dedupFileName,
	}

	tReply := &utils.StartTaskReply{}
	err = utils.CallRPC(targetAddr, "Node.StartTask", taskArgs, tReply)

	if err == nil && tReply.Success {
		// Update Global Map
		node.TaskMapLock.Lock()
		if _, ok := node.GlobalTaskMap[stageID]; !ok {
			node.GlobalTaskMap[stageID] = &StageInfo{Tasks: make(map[int]TaskInfo)}
		}
		node.GlobalTaskMap[stageID].Tasks[newTaskID] = TaskInfo{
			StageID:      stageID,
			TaskID:       newTaskID,
			VMAddr:       targetAddr,
			VMName:       vmName,
			PID:          tReply.PID,
			OpExe:        opExe,
			LogFileName:  tReply.LogFileName,
			OriginalArgs: taskArgs,
			DedupFile:    dedupFileName,
		}
		node.TaskMapLock.Unlock()
		node.FinishLock.Lock()
		node.TotalTasks++
		node.FinishLock.Unlock()

		node.LogLock.RLock()
		if node.LeaderLog != nil {
			node.LeaderLog.TaskLogger.Printf("[ScaleUp] Stage %d Task %d on %s (PID %d) | Exe: %s | Log: %s\n",
				taskArgs.StageID, taskArgs.TaskID, displayHost, tReply.PID, taskArgs.OpExe, tReply.LogFileName)
		}
		node.LogLock.RUnlock()

		// Broadcast new downstream to upstream tasks
		node.broadcastUpdateDownstream(stageID - 1)
		// Broadcast new upstream count to downstream tasks
		node.broadcastUpdateUpstream(stageID + 1)
	} else {
		fmt.Printf("[Autoscale] Failed to start task: %v\n", err)
		node.LogLock.RLock()
		if node.LeaderLog != nil {
			node.LeaderLog.ErrorLogger.Printf("[TASK_START_FAIL] Stage %d Task %d on %s | Error: %v | ReplySuccess: %v\n",
				taskArgs.StageID, taskArgs.TaskID, targetAddr, err, tReply.Success)
		}
		node.LogLock.RUnlock()
	}
}

func (node *Node) scaleDown(args *utils.LeaderStartArgs, stageID int) {
	node.ScaleCooldown = time.Now().Add(3 * time.Second)

	// Find Victim (Max ID)
	node.TaskMapLock.RLock()
	maxID := -1
	var victimVMAddr string
	var victimPID int
	survivorAddr := ""
	survivorTaskID := 0
	var orphanLog string
	var victimOpExe string
	var victimVMName string
	if stageInfo, ok := node.GlobalTaskMap[stageID]; ok {
		for tID, info := range stageInfo.Tasks {
			if tID > maxID {
				maxID = tID
				victimVMAddr = info.VMAddr
				victimPID = info.PID
				orphanLog = info.DedupFile
				victimOpExe = info.OpExe
				victimVMName = info.VMName
			}
			if tID == 0 {
				survivorAddr = info.VMAddr
			}
		}
	}
	node.TaskMapLock.RUnlock()

	if maxID == -1 || maxID == 0 {
		return
	}
	displayHost := fmt.Sprintf("%s (%s)", victimVMName, victimVMAddr)

	fmt.Printf("[Autoscale] <<< Scaling Down <<< Killing Stage %d Task %d on %s\n", stageID, maxID, displayHost)

	// Remove from Global Map
	node.TaskMapLock.Lock()
	if stageInfo, ok := node.GlobalTaskMap[stageID]; ok {
		delete(stageInfo.Tasks, maxID)
	}
	node.TaskMapLock.Unlock()

	node.FinishLock.Lock()
	node.TotalTasks--
	node.FinishLock.Unlock()

	// Remove from TaskRates map
	node.TaskRatesLock.Lock()
	if rates, ok := node.TaskRates[stageID]; ok {
		delete(rates, maxID)
	}
	node.TaskRatesLock.Unlock()

	node.LogLock.RLock()
	if node.LeaderLog != nil {
		node.LeaderLog.TaskLogger.Printf("[ScaleDown] Stage %d Task %d on %s (PID %d) | Exe: %s | Orphan: %s\n",
			stageID, maxID, displayHost, victimPID, victimOpExe, orphanLog)
	}
	node.LogLock.RUnlock()

	// Prevent data loss by scale down (waitgroup blocking)
	// Broadcast new downstream to upstream tasks
	node.broadcastUpdateDownstream(stageID - 1)
	// Broadcast new upstream count to downstream tasks
	node.broadcastUpdateUpstream(stageID + 1)

	// kill the task
	killArgs := &utils.KillTaskArgs{PID: victimPID}
	go utils.CallRPC(victimVMAddr, "Node.KillTask", killArgs, &utils.KillTaskReply{})
	//fmt.Printf("[Autoscale] Assigning orphan log %s to Task 0\n", orphanLog)

	recoverArgs := &utils.ProcessOrphanArgs{
		StageID:       stageID,
		TaskID:        survivorTaskID,
		OrphanLogFile: orphanLog,
	}

	go func() {
		reply := &utils.ProcessOrphanReply{}
		err := utils.CallRPC(survivorAddr, "Node.ProcessOrphanLog", recoverArgs, reply)
		if err != nil {
			fmt.Printf("[Autoscale Error] RPC Call Failed: %v\n", err)
		} else if !reply.Success {
			fmt.Printf("[Autoscale Error] Orphan Adoption Failed: %s\n", reply.ErrorMessage)
		} else {
			fmt.Printf("[Autoscale] Orphan Adoption Started Successfully for %s\n", orphanLog)
		}
	}()
}

func (node *Node) broadcastUpdateDownstream(targetStageID int) {
	if targetStageID < 1 {
		return
	}

	// collect new downstream list
	nextStageID := targetStageID + 1
	newDownstream := node.getSortedStageWorkers(nextStageID)

	//fmt.Printf("[Autoscale] Broadcasting new downstream list (count=%d) to Stage %d\n", len(newDownstream), targetStageID)
	var wg sync.WaitGroup
	// 2.Broadcast to next stage with new downstream tasks
	node.TaskMapLock.RLock()
	if stageInfo, ok := node.GlobalTaskMap[targetStageID]; ok {
		for _, info := range stageInfo.Tasks {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				args := &utils.UpdateDownstreamArgs{
					StageID:         targetStageID,
					DownstreamTasks: newDownstream,
				}
				utils.CallRPC(addr, "Node.UpdateDownstream", args, &utils.UpdateDownstreamReply{})
			}(info.VMAddr)
		}
	}
	node.TaskMapLock.RUnlock()
	wg.Wait()
}

func (node *Node) getLeastLoadedWorker(aliveNodes []string) string {
	if len(aliveNodes) == 0 {
		return ""
	}

	// Count tasks per VM
	vmLoad := make(map[string]int)
	for _, addr := range aliveNodes {
		vmLoad[addr] = 0
	}

	node.TaskMapLock.RLock()
	for _, stageInfo := range node.GlobalTaskMap {
		for _, info := range stageInfo.Tasks {
			vmLoad[info.VMAddr]++
		}
	}
	node.TaskMapLock.RUnlock()

	// Find min
	minLoad := 999999
	bestNode := aliveNodes[0]

	for _, addr := range aliveNodes {
		load := vmLoad[addr]
		if load < minLoad {
			minLoad = load
			bestNode = addr
		}
	}
	return bestNode
}

func (node *Node) getSortedStageWorkers(stageID int) []string {
	node.TaskMapLock.RLock()
	defer node.TaskMapLock.RUnlock()

	var tasks []TaskInfo
	if stageInfo, ok := node.GlobalTaskMap[stageID]; ok {
		for _, info := range stageInfo.Tasks {
			tasks = append(tasks, info)
		}
	}

	// sort for partition
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].TaskID < tasks[j].TaskID
	})

	var workers []string
	for _, t := range tasks {
		workers = append(workers, t.VMAddr)
	}
	return workers
}

func (node *Node) getStageTaskCount(stageID int) int {
	node.TaskMapLock.RLock()
	defer node.TaskMapLock.RUnlock()
	if stageInfo, ok := node.GlobalTaskMap[stageID]; ok {
		return len(stageInfo.Tasks)
	}
	return 0
}

func (node *Node) UpdateUpstream(args *utils.UpdateUpstreamArgs, reply *utils.UpdateUpstreamReply) error {
	node.rsTaskMutex.Lock()
	defer node.rsTaskMutex.Unlock()

	count := 0
	for key, task := range node.rsTasks {
		var sID, tID int
		if n, err := fmt.Sscanf(key, "%d:%d", &sID, &tID); err == nil && n == 2 {
			if sID == args.StageID {
				task.EOSLock.Lock()
				task.NumUpstream = args.NumUpstream
				task.EOSLock.Unlock()
				count++
			}
		}
	}
	fmt.Printf("[Node] Updated NumUpstream to %d for %d tasks in Stage %d\n", args.NumUpstream, count, args.StageID)
	reply.Success = true
	return nil
}

func (node *Node) broadcastUpdateUpstream(targetStageID int) {
	// Get new NumUpstream
	if targetStageID < 1 {
		return
	}
	newNumUpstream := node.getStageTaskCount(targetStageID - 1)
	node.TaskMapLock.RLock()
	if stageInfo, ok := node.GlobalTaskMap[targetStageID]; ok {
		for _, info := range stageInfo.Tasks {
			go func(addr string) {
				args := &utils.UpdateUpstreamArgs{
					StageID:     targetStageID,
					NumUpstream: newNumUpstream,
				}
				utils.CallRPC(addr, "Node.UpdateUpstream", args, &utils.UpdateUpstreamReply{})
			}(info.VMAddr)
		}
	}
	node.TaskMapLock.RUnlock()
}

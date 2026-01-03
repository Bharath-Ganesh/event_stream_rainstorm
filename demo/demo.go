package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/constants"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/member"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/utils"
)

type CliMember struct {
	ID      string `json:"id"`
	Address string `json:"addr"`
	State   string `json:"state"`
}

type CliResult struct {
	NodeID        string
	Nodename      string
	Output        string
	EachLineCount int
}

func ExecCreate(localFileName string, hyDFSFileName string) {
	localFile, err := os.ReadFile(localFileName)
	if err != nil {
		fmt.Printf("Cannot read the local file: %s\n", localFileName)
		return
	}
	replicaInfo, err := utils.GetReplica(hyDFSFileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	primary := replicaInfo[0]
	nonPrimary := []string{}
	for i := 1; i < constants.ReplicaNum; i++ {
		nonPrimary = append(nonPrimary, replicaInfo[i].Address)
	}

	vmName := utils.GetSelfVM()
	opID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), vmName)
	args := &utils.WriteFileArgs{
		HyDFSFileName: hyDFSFileName,
		LocalFile:     localFile,
		NonPrimary:    nonPrimary,
		OperationID:   opID,
	}

	reply := &utils.WriteFileReply{}
	// The primary replica address is used from the create
	err = utils.CallRPC(primary.Address, "Node.CreateFile", args, reply)
	if err != nil {
		fmt.Println("Cannot call RPC CreateFile on primary replica")
		return
	}
	if reply.Success {
		fmt.Println("Create Command complete.")
		for i := 0; i < constants.ReplicaNum; i++ {
			rpcAddr := replicaInfo[i].Address
			vmName, check := utils.GetVMName(rpcAddr)
			if !check {
				continue
			}
			fmt.Printf("%s (%s)\n", vmName, replicaInfo[i].Address)
		}
		fmt.Println()
	} else {
		fmt.Printf("Create command failed. Error from server: %s\n", reply.ErrorMessage)
	}
}

func ExecGet(hyDFSFileName string, localFileName string) {
	replicaInfo, err := utils.GetReplica(hyDFSFileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	args := &utils.GetFileArgs{
		HyDFSFileName: hyDFSFileName,
	}
	var finalReply *utils.GetFileReply
	getSuccess := false
	for _, replica := range replicaInfo {
		reply := &utils.GetFileReply{}
		err := utils.CallRPC(replica.Address, "Node.GetFile", args, reply)
		if err != nil {
			fmt.Printf("CallRPC: Get %s in replica %s failed\n", hyDFSFileName, replica.Address)
			continue
		}
		// Only wait for the first successful reply
		if reply.Success {
			getSuccess = true
			finalReply = reply
			break
		} else {
			fmt.Printf("RPC Reply Failed: %s in replica %s", hyDFSFileName, replica.Address)
			continue
		}
	}
	if getSuccess {
		err = os.WriteFile(localFileName, finalReply.FileData, 0644)
		if err != nil {
			fmt.Printf("Cannot write localfile %s\n", localFileName)
			return
		}
	} else {
		fmt.Printf("Get command failed for all replicas\n")
	}
	fmt.Println("Get command finish.")
}

// Get file from a specific replica address and save locally
// replicaAddress can be either a VM name (e.g., "VM10") or an IP:port address (e.g., "172.22.155.19:9081")
func ExecGetFromReplica(replicaAddress string, hyDFSFileName string, localFileName string) {
	// Resolve VM name to IP:RPCPort if needed
	var rpcAddress string
	if targetIP, ok := utils.VMNameToIP(replicaAddress); ok {
		// It's a VM name, resolve to IP and add RPC port
		rpcAddress = targetIP + ":" + constants.RPCPort
	} else {
		// Assume it's already an IP:port address
		rpcAddress = replicaAddress
	}

	args := &utils.GetFileArgs{HyDFSFileName: hyDFSFileName}
	reply := &utils.GetFileReply{}
	err := utils.CallRPC(rpcAddress, "Node.GetFile", args, reply)
	if err != nil {
		fmt.Printf("CallRPC GetFile to %s failed: %v\n", rpcAddress, err)
		return
	}
	if !reply.Success {
		fmt.Printf("Get from replica failed for %s at %s\n", hyDFSFileName, rpcAddress)
		return
	}
	if err := os.WriteFile(localFileName, reply.FileData, 0644); err != nil {
		fmt.Printf("Cannot write local file %s: %v\n", localFileName, err)
		return
	}
	fmt.Println("Get from replica finished.")
}

func ExecAppend(localFileName string, hyDFSFileName string) {
	localFile, err := os.ReadFile(localFileName)
	if err != nil {
		fmt.Printf("Cannot read the localfile %s through append command\n", localFile)
		return
	}
	replicaInfo, err := utils.GetReplica(hyDFSFileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	primary := replicaInfo[0]
	nonPrimary := []string{}
	for i := 1; i < constants.ReplicaNum; i++ {
		nonPrimary = append(nonPrimary, replicaInfo[i].Address)
	}
	vmName := utils.GetSelfVM()
	opID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), vmName)
	args := &utils.WriteFileArgs{
		HyDFSFileName: hyDFSFileName,
		LocalFile:     localFile,
		NonPrimary:    nonPrimary,
		OperationID:   opID,
	}

	fmt.Printf("[Client][Append] primary=%s nonPrimary=%v file=%s local=%s bytes=%d\n", primary.Address, nonPrimary, hyDFSFileName, localFileName, len(localFile))
	reply := &utils.WriteFileReply{}
	err = utils.CallRPC(primary.Address, "Node.AppendFile", args, reply)
	if err != nil {
		fmt.Println("Cannot call RPC AppendFile on primary replica")
		return
	}
	if reply.Success {
		fmt.Println("[Client][Append] complete.")
		for i := 0; i < constants.ReplicaNum; i++ {
			rpcAddr := replicaInfo[i].Address
			vmName, check := utils.GetVMName(rpcAddr)
			if !check {
				continue
			}
			fmt.Printf("%s (%s)\n", vmName, replicaInfo[i].Address)
		}
		fmt.Println()
	} else {
		fmt.Printf("Append command failed. Error from server: %s\n", reply.ErrorMessage)
	}
}

func ExecLs(hyDFSFileName string) {
	fileID := utils.HashFunc(hyDFSFileName)
	fmt.Printf("hyDFSFileName %s FileID: %d\n", hyDFSFileName, fileID)
	replicaInfo, err := utils.GetReplica(hyDFSFileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Replicas-------------")
	for i := 0; i < constants.ReplicaNum; i++ {
		rpcAddr := replicaInfo[i].Address
		vmName, check := utils.GetVMName(rpcAddr)
		if !check {
			continue
		}
		ringID := replicaInfo[i].NodeID
		fmt.Printf("RingID %d in %s (%s)\n", ringID, vmName, replicaInfo[i].Address)
	}
	fmt.Println()
}

func ExecListMemIDs() {
	host, err := os.Hostname()
	if err != nil {
		fmt.Printf("Cannot get hostname\n")
		return
	}
	address := host + ":" + constants.CommandPort
	memberListJSON, err := utils.SendCommand(address, "mem_list_json")
	if err != nil {
		fmt.Printf("Cannot get memberlist\n")
		return
	}
	var currentMembers []member.Member
	err = json.Unmarshal([]byte(memberListJSON), &currentMembers)
	if err != nil {
		fmt.Printf("Cannot Marshal the memberlist\n")
		return
	}
	ringNodes := utils.GetRingInfo(currentMembers)

	sort.Slice(ringNodes, func(i, j int) bool {
		return ringNodes[i].NodeID < ringNodes[j].NodeID
	})

	for _, node := range ringNodes {
		rpcAddr := node.Address
		ringID := node.NodeID
		vmName, check := utils.GetVMName(rpcAddr)
		if !check {
			continue
		}
		fmt.Printf("RingID %d in %s (%s)\n", ringID, rpcAddr, vmName)
	}
}

func ExecListStore() {
	host, _ := os.Hostname()
	address := host + ":" + constants.CommandPort
	response, err := utils.SendCommand(address, "liststore")
	if err != nil {
		fmt.Printf("Error for liststore\n")
		return
	}
	fmt.Print(response)
}

func ExecDelete(fileList []string) {
	for _, filename := range fileList {
		err := os.Remove(filename)
		if err != nil {
			fmt.Printf("Cannot delete the file %s\n", filename)
		} else {
			fmt.Printf("Delete the file %s\n", filename)
		}
	}
}

func ExecMerge(hyDFSFileName string) {
	replicaInfo, err := utils.GetReplica(hyDFSFileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	primary := replicaInfo[0]

	// We need new Arg/Reply structs for Merge
	args := &utils.MergeFileArgs{
		HyDFSFileName: hyDFSFileName,
	}
	reply := &utils.MergeFileReply{}

	fmt.Printf("[Client] Sending merge request for %s to primary %s...\n", hyDFSFileName, primary.Address)
	startTime := time.Now()
	err = utils.CallRPC(primary.Address, "Node.MergeFile", args, reply)
	if err != nil {
		fmt.Printf("[Client] ERROR: Cannot call RPC MergeFile on primary replica: %v\n", err)
		return
	}
	elapsed := time.Since(startTime)
	if reply.Success {
		fmt.Printf("[Client] Merge Command complete. Total time: %s\n", elapsed)
	} else {
		fmt.Printf("[Client] Merge command failed. Total time: %s. Error from server: %s\n", elapsed, reply.ErrorMessage)
	}
}

func ExecMultiAppend(hyDFSFileName string, vmListStr string, fileListStr string) {
	vmNames := strings.Split(vmListStr, ",")
	localFiles := strings.Split(fileListStr, ",")

	if len(vmNames) != len(localFiles) {
		fmt.Println("Error: The number of VMs must match the number of local filenames.")
		return
	}

	var wg sync.WaitGroup
	fmt.Printf("[MultiAppend] Initiating %d concurrent appends for %s...\n", len(vmNames), hyDFSFileName)
	startTime := time.Now()

	for i := 0; i < len(vmNames); i++ {
		wg.Add(1)
		go func(targetVMName string, localFile string) {
			defer wg.Done()
			// Resolve VM name (case-insensitive) to IP using existing mapping
			targetIP, ok := utils.VMNameToIP(targetVMName)
			if !ok {
				fmt.Printf("[MultiAppend] Error: Unknown VM name %s\n", targetVMName)
				return
			}

			// Build RPC address for target VM
			targetRPCAddress := targetIP + ":" + constants.RPCPort

			// Send trigger RPC to target VM
			// The target VM will read the local file and perform the append
			triggerArgs := &utils.TriggerAppendArgs{
				LocalFileName: localFile,
				HyDFSFileName: hyDFSFileName,
			}

			fmt.Printf("[MultiAppend][Send] vm=%s rpcAddr=%s trigger local=%s hyDFS=%s\n", targetVMName, targetRPCAddress, localFile, hyDFSFileName)

			// Make trigger RPC call to target VM's RPC port
			reply := &utils.TriggerAppendReply{}
			err := utils.CallRPC(targetRPCAddress, "Node.TriggerAppend", triggerArgs, reply)

			if err != nil {
				fmt.Printf("[MultiAppend][Ack] vm=%s status=FAILED err=%s\n", targetVMName, err)
			} else if reply.Success {
				fmt.Printf("[MultiAppend][Ack] vm=%s status=OK\n", targetVMName)
			} else {
				fmt.Printf("[MultiAppend][Ack] vm=%s status=FAILED err=%s\n", targetVMName, reply.ErrorMessage)
			}
		}(vmNames[i], localFiles[i])
	}

	// Wait for all VMS appends to finish
	wg.Wait()
	elapsed := time.Since(startTime)
	fmt.Printf("[MultiAppend] All appends finished. Total time: %s\n", elapsed)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("please enter the command.")
		os.Exit(1)
	}
	command := os.Args[1]

	switch command {
	case "create":
		if len(os.Args) != 4 {
			fmt.Println("create localfilename HyDFSfilename")
			os.Exit(1)
		}
		localFileName := os.Args[2]
		HyDFSFileName := os.Args[3]
		ExecCreate(localFileName, HyDFSFileName)
	case "get":
		if len(os.Args) != 4 {
			fmt.Println("get HyDFSfilename localfilename")
			os.Exit(1)
		}
		HyDFSFileName := os.Args[2]
		localFileName := os.Args[3]
		ExecGet(HyDFSFileName, localFileName)
	case "getfromreplica":
		if len(os.Args) != 5 {
			fmt.Println("getfromreplica VMaddress HyDFSfilename localfilename")
			os.Exit(1)
		}
		replicaAddress := os.Args[2]
		HyDFSFileName := os.Args[3]
		localFileName := os.Args[4]
		ExecGetFromReplica(replicaAddress, HyDFSFileName, localFileName)
	case "ls":
		if len(os.Args) != 3 {
			fmt.Println("ls HyDFSfilename")
			os.Exit(1)
		}
		HyDFSFileName := os.Args[2]
		ExecLs(HyDFSFileName)
	case "list_mem_ids":
		if len(os.Args) != 2 {
			fmt.Println("list_mem_ids")
			os.Exit(1)
		}
		ExecListMemIDs()
	case "liststore":
		if len(os.Args) != 2 {
			fmt.Println("liststore")
			os.Exit(1)
		}
		ExecListStore()
	case "delete":
		if len(os.Args) < 3 {
			fmt.Println("delete filename1 [filename2] ...")
			os.Exit(1)
		}
		ExecDelete(os.Args[2:])
	case "append":
		if len(os.Args) != 4 {
			fmt.Println("append localfilename HyDFSfilename")
			os.Exit(1)
		}
		localFileName := os.Args[2]
		HyDFSFileNmae := os.Args[3]
		ExecAppend(localFileName, HyDFSFileNmae)
	case "multiappend":
		if len(os.Args) != 5 {
			fmt.Println("Usage: multiappend <HyDFSfilename> <vm_list> <localfile_list>")
			fmt.Println("Example: multiappend foo.txt vm2,vm5,vm7,vm8 fileC.txt,fileD.txt,fileE.txt,fileF.txt")
			os.Exit(1)
		}
		HyDFSFileName := os.Args[2]
		vmListStr := os.Args[3]
		fileListStr := os.Args[4]
		ExecMultiAppend(HyDFSFileName, vmListStr, fileListStr)
	case "RainStorm":
		if len(os.Args) < 10 {
			fmt.Println("RainStorm <Nstages> <Ntasks> <op1> <args1> ... <src> <dest> <eo> <auto>...")
			os.Exit(1)
		}
		numStages, err := strconv.Atoi(os.Args[2])
		if err != nil || numStages <= 0 {
			fmt.Println("Invalid Nstages")
			os.Exit(1)
		}
		numTasks, err := strconv.Atoi(os.Args[3])
		if err != nil || numTasks <= 0 {
			fmt.Println("Invalid Ntasks")
			os.Exit(1)
		}
		opExes := []string{}
		opArgs := []string{}
		argIndex := 4
		for i := 0; i < numStages; i++ {
			opExes = append(opExes, os.Args[argIndex])
			opArgs = append(opArgs, os.Args[argIndex+1])
			argIndex += 2
		}
		srcFile := os.Args[argIndex]
		destFile := os.Args[argIndex+1]
		exactlyOnce, errEO := strconv.ParseBool(os.Args[argIndex+2])
		if errEO != nil {
			fmt.Println("Error: exactly_once must be a boolean (true/false/1/0)")
			os.Exit(1)
		}
		autoScale, errAS := strconv.ParseBool(os.Args[argIndex+3])
		if errAS != nil {
			fmt.Println("Error: autoscale_enabled must be a boolean (true/false/1/0)")
			os.Exit(1)
		}
		argIndex += 4
		inputRate := 100
		lw := 0
		hw := 0
		if autoScale {
			if len(os.Args) < argIndex+1 {
				fmt.Println("Error: input_rate must be provided when autoscale_enabled is true")
				os.Exit(1)
			}
			inputRate, err = strconv.Atoi(os.Args[argIndex])
			if err != nil || inputRate <= 0 {
				fmt.Println("Invalid input_rate")
				os.Exit(1)
			}
			lw, err = strconv.Atoi(os.Args[argIndex+1])
			if err != nil || lw <= 0 {
				fmt.Println("Invalid lw")
				os.Exit(1)
			}
			hw, err = strconv.Atoi(os.Args[argIndex+2])
			if err != nil || hw <= 0 {
				fmt.Println("Invalid hw")
				os.Exit(1)
			}
		}
		if autoScale {
			fmt.Printf("RainStorm Config: Stages=%d, Tasks=%d, EO=%t, Auto=%t, Rate=%d, LW=%d, HW=%d\n",
				numStages, numTasks, exactlyOnce, autoScale, inputRate, lw, hw)
		} else {
			fmt.Printf("RainStorm Config: Stages=%d, Tasks=%d, EO=%t\n",
				numStages, numTasks, exactlyOnce)
		}
		rsID := fmt.Sprintf("rs-%d", time.Now().Unix())
		jobArgs := &utils.LeaderStartArgs{
			RSID:        rsID,
			NumStages:   numStages,
			NumTasks:    numTasks,
			OpExes:      opExes,
			OpArgs:      opArgs,
			SrcFile:     srcFile,
			DestFile:    destFile,
			ExactlyOnce: exactlyOnce,
			Autoscale:   autoScale,
			InputRate:   inputRate,
			LW:          lw,
			HW:          hw,
		}

		leaderIP, _ := utils.VMNameToIP("VM1")
		leaderRPC := leaderIP + ":" + constants.RPCPort

		fmt.Printf("[CLI] Sending Job to Leader (%s)...\n", leaderRPC)
		reply := &utils.LeaderStartReply{}
		err = utils.CallRPC(leaderRPC, "Node.StartRainStorm", jobArgs, reply)

		if err != nil {
			fmt.Println("StartRainStorm RPC Failed:", err)
		} else if !reply.Success {
			fmt.Println("StartRainStorm Job Failed:", reply.ErrorMessage)
		} else {
			fmt.Println("StartRainStorm Job Started Successfully.")
		}
	case "list_tasks":
		leaderIP, _ := utils.VMNameToIP("VM1")
		leaderRPC := leaderIP + ":" + constants.RPCPort
		var output string
		utils.CallRPC(leaderRPC, "Node.ListTasks", &struct{}{}, &output)
		fmt.Print(output)
	case "kill_task":
		if len(os.Args) < 4 {
			fmt.Println("Usage: kill_task <VM_Address/Name> <PID>")
			return
		}

		targetVMStr := os.Args[2]
		pidStr := os.Args[3]
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			fmt.Println("Error: PID must be an integer")
			return
		}
		// Turn VM name or address into RPC address if needed
		var targetRPCAddr string
		if ip, ok := utils.VMNameToIP(targetVMStr); ok {
			targetRPCAddr = ip + ":" + constants.RPCPort
		} else {
			if strings.Contains(targetVMStr, ":") {
				targetRPCAddr = targetVMStr
			} else {
				targetRPCAddr = targetVMStr + ":" + constants.RPCPort
			}
		}
		args := &utils.KillTaskArgs{PID: pid}
		reply := &utils.KillTaskReply{}

		fmt.Printf("Sending KillTask command to %s for PID %d...\n", targetVMStr, pid)
		err = utils.CallRPC(targetRPCAddr, "Node.KillTask", args, reply)

		if err != nil {
			fmt.Printf("Kill Task RPC Error: %v\n", err)
		} else if !reply.Success {
			fmt.Printf("Kill Task Failed: %s\n", reply.ErrorMessage)
		} else {
			fmt.Println("Kill Task Success.")
		}
	case "switch":
		if len(os.Args) != 4 {
			fmt.Println("The number of paras are not correct")
			return
		}
		introAddress := "fa25-cs425-8201.cs.illinois.edu:" + constants.CommandPort
		protocol := os.Args[2]
		suspicion := os.Args[3]
		if protocol != "gossip" && protocol != "ping" {
			fmt.Println("Please give valid protocol")
			return
		}
		if suspicion != "suspect" && suspicion != "nosuspect" {
			fmt.Println("Please give valid suspicion mechanism")
			return
		}
		commandString := fmt.Sprintf("switch %s %s\n", protocol, suspicion)
		_, err := utils.SendCommand(introAddress, commandString)
		if err != nil {
			os.Exit(1)
		}
	case "list_mem":
		if len(os.Args) != 2 {
			fmt.Println("The number of paras are not correct")
			return
		}
		host, err := os.Hostname()
		if err != nil {
			fmt.Println("cannot get hostname")
		}
		Address := host + ":" + constants.CommandPort
		memberListString, err := utils.SendCommand(Address, "list_mem")
		if err != nil {
			os.Exit(1)
		}
		fmt.Print(memberListString)
	case "display_protocol":
		if len(os.Args) != 2 {
			fmt.Println("The number of paras for display protocol are not correct")
			return
		}
		host, err := os.Hostname()
		if err != nil {
			fmt.Println("cannot get hostname")
		}
		Address := host + ":" + constants.CommandPort
		memberListString, err := utils.SendCommand(Address, "display_protocol")
		if err != nil {
			os.Exit(1)
		}
		fmt.Print(memberListString)
	case "display_suspects":
		if len(os.Args) != 2 {
			fmt.Println("The number of paras for display suspects are not correct")
			return
		}
		host, err := os.Hostname()
		if err != nil {
			fmt.Println("cannot get hostname")
		}
		Address := host + ":" + constants.CommandPort
		memberListString, err := utils.SendCommand(Address, "display_suspects")
		if err != nil {
			os.Exit(1)
		}
		fmt.Print(memberListString)
	case "list_self":
		if len(os.Args) != 2 {
			fmt.Println("The number of paras for list self are not correct")
			return
		}
		host, err := os.Hostname()
		if err != nil {
			fmt.Println("cannot get hostname")
		}
		Address := host + ":" + constants.CommandPort
		memberListString, err := utils.SendCommand(Address, "list_self")
		if err != nil {
			os.Exit(1)
		}
		fmt.Print(memberListString)
	case "leave":
		if len(os.Args) != 2 {
			fmt.Println("The number of paras for leave are not correct")
			return
		}
		host, err := os.Hostname()
		if err != nil {
			fmt.Println("cannot get hostname")
		}
		Address := host + ":" + constants.CommandPort
		_, err = utils.SendCommand(Address, "leave")
		if err != nil {
			os.Exit(1)
		}
		fmt.Println("Nodes are leaving")
	case "merge":
		if len(os.Args) != 3 {
			fmt.Println("merge HyDFSfilename")
			os.Exit(1)
		}
		HyDFSFileName := os.Args[2]
		ExecMerge(HyDFSFileName)
	case "grep":
		if len(os.Args) < 3 {
			fmt.Println("The number of paras for grep are not correct")
			return
		}
		grepArg := os.Args[2:]
		cInArgs := false
		for _, grepargs := range grepArg {
			if grepargs == "-c" {
				cInArgs = true
				break
			}
		}
		introAddress := "fa25-cs425-8201.cs.illinois.edu:" + constants.CommandPort
		memberListJSON, err := utils.SendCommand(introAddress, "mem_list_json")
		if err != nil {
			fmt.Println("Cannot send getting the member list command")
			os.Exit(1)
		}
		var currentMembers []CliMember
		err = json.Unmarshal([]byte(memberListJSON), &currentMembers)
		if err != nil {
			fmt.Println("Cannot Marshal the memberlist")
			os.Exit(1)
		}
		getGrepResult(currentMembers, grepArg, cInArgs)
	default:
		//log the command
		fmt.Println("Command not found: ", command)
		os.Exit(1)
	}
}

// MP1 grep logger from VM1 - VM10
func getGrepResult(memberlist []CliMember, grepArg []string, cInArgs bool) {
	lenMember := len(memberlist)
	resultChan := make(chan CliResult, lenMember)
	var waitGroup sync.WaitGroup
	ConcatString := strings.Join(grepArg, " ")
	grepCommand := "grep " + ConcatString
	for _, eachMember := range memberlist {
		if eachMember.State == constants.Alive || eachMember.State == constants.Suspected {
			waitGroup.Add(1)
			go func(m CliMember) {
				defer waitGroup.Done()
				host := strings.Split(m.Address, ":")[0]
				sentAddr := fmt.Sprintf("%s:%s", host, constants.CommandPort)
				grepOutput, err := utils.SendCommand(sentAddr, grepCommand)
				if err != nil {
					return
				}
				if len(grepOutput) > 0 {
					resultSlice := CliResult{
						NodeID: m.ID,
						Output: grepOutput,
					}
					if cInArgs {
						lineCount, err := strconv.Atoi(strings.TrimSpace(grepOutput))
						if err == nil {
							resultSlice.EachLineCount = lineCount
						}
					} else {
						splitOutput := strings.Split(strings.TrimSpace(grepOutput), "\n")
						resultSlice.EachLineCount = len(splitOutput)
					}
					resultChan <- resultSlice
				}
			}(eachMember)
		}
	}
	waitGroup.Wait()
	close(resultChan)
	sumTotalLine := 0
	collectResult := []CliResult{}
	for result := range resultChan {
		collectResult = append(collectResult, result)
	}
	fmt.Println("Here's the result ---------")
	if !cInArgs {
		for _, eachResult := range collectResult {
			lines := strings.Split(strings.TrimSpace(eachResult.Output), "\n")
			for _, eachline := range lines {
				fmt.Printf("[%s] %s\n", eachResult.NodeID, eachline)
			}
		}
	}
	for _, eachresult := range collectResult {
		fmt.Printf("[%s] totallines: %d\n", eachresult.NodeID, eachresult.EachLineCount)
		sumTotalLine += eachresult.EachLineCount
	}
	fmt.Println("total number of matching lines summed across all VMs: ", sumTotalLine)
	fmt.Println("End-------------------------")
}

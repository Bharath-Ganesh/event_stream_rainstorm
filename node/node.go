package node

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/constants"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/member"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/message"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/streamlog"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/utils"
	vmlog "gitlab.engr.illinois.edu/yonghan4/mp4_g82/vmlog"
)

type Node struct {
	ID             string
	memberlist     *MemberList
	introducerAddr string
	udpconn        *net.UDPConn
	ackchan        map[string]chan bool
	ackMutex       sync.Mutex
	protocol       string
	suspicion      string
	switchmutex    sync.Mutex
	bandwithMutex  sync.Mutex
	totalBytes     int64
	startTime      time.Time
	startTineMutex sync.Mutex
	commandPort    string
	storagePath    string
	logPath        string
	vmName         string
	host           string
	fileMetadata   map[string]*FileMetadata // Map from hyDFSFileName to its lock
	metaLock       sync.RWMutex             // A lock to protect the map itself
	// MP4 stream processing
	rsTasks       map[string]*RainStormTask
	TaskPidMap    map[int]string // PID -> TaskKey ("StageID:TaskID")
	rsTaskMutex   sync.Mutex
	LeaderLog     *streamlog.StreamLoggers
	LogLock       sync.RWMutex
	GlobalTaskMap map[int]*StageInfo // Key: StageID
	TaskMapLock   sync.RWMutex
	TotalTasks    int
	FinishedTasks int
	FinishLock    sync.Mutex
	// MP4 autoscale
	TaskRates     map[int]map[int]float64 // StageID -> TaskID -> Rate
	TaskRatesLock sync.RWMutex

	AutoLW            int
	AutoHW            int
	ScaleCooldown     time.Time
	AutoscaleStopChan chan struct{}
}

type FileMetadata struct {
	lock sync.Mutex
}

func NewNode(port string, introducerAddr string, commandPort string) (*Node, error) {
	// Get actual IP address
	actualHostname, err := os.Hostname()
	if err != nil {
		actualHostname = "localhost"
	}

	var bindHostname string
	if strings.Contains(actualHostname, "fa25-cs425") {
		conn, err := net.Dial("udp", "8.8.8.8:80")
		if err != nil {
			bindHostname = "0.0.0.0"
		} else {
			localAddr := conn.LocalAddr().(*net.UDPAddr)
			bindHostname = localAddr.IP.String()
			conn.Close()
		}
	} else {
		bindHostname = "localhost" // Local environment
	}

	vmName, check := utils.IpToVMName[bindHostname]
	if !check {
		fmt.Println("Cannot get VM Name")
		return nil, err
	}

	ip := bindHostname + ":" + port
	udpAddr, err := net.ResolveUDPAddr("udp", ip)
	if err != nil {
		fmt.Println("Cannot resolve UDP address from ", ip)
		return nil, err
	}
	udpconn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Println("Cannot listen through udp with ip: ", ip)
		return nil, err
	}
	uniqueTime := time.Now().UnixMilli()
	machineID := fmt.Sprintf("%s:%d", ip, uniqueTime)
	memberList := NewMemberList()
	nodeItSelf := member.Member{
		ID:               machineID,
		Address:          ip,
		HeartBeatCounter: 0,
		Incarnation:      0,
		State:            constants.Alive,
		LastUpdateTime:   time.Now(),
	}
	memberList.memberlist[machineID] = &nodeItSelf
	// MP3 test 1
	storagePath := "./hydfs"
	logPath := "./hydfs_log"
	// MP4 logger
	streamLogPath := "./slog"
	os.RemoveAll(storagePath)
	os.RemoveAll(logPath)
	os.RemoveAll(streamLogPath)
	node := &Node{
		ID:             machineID,
		memberlist:     memberList,
		introducerAddr: introducerAddr,
		udpconn:        udpconn,
		ackchan:        make(map[string]chan bool),
		protocol:       constants.ProtocolPingAck,
		suspicion:      constants.SuspecionFalse,
		totalBytes:     0,
		startTime:      time.Now(),
		commandPort:    commandPort,
		// MP3 test 1
		storagePath:   storagePath,
		logPath:       logPath,
		vmName:        vmName,
		host:          bindHostname,
		fileMetadata:  make(map[string]*FileMetadata),
		rsTasks:       make(map[string]*RainStormTask),
		TaskPidMap:    make(map[int]string),
		GlobalTaskMap: make(map[int]*StageInfo),
		TaskRates:     make(map[int]map[int]float64),
	}
	// MP3 test 1
	os.MkdirAll(node.storagePath, 0755)
	os.MkdirAll(node.logPath, 0755)
	return node, nil
}

func (node *Node) Switch(protocol string, suspicion string) {
	if protocol != constants.ProtocolGossip && protocol != constants.ProtocolPingAck {
		fmt.Println("Invalid protocol")
		return
	}
	if suspicion != constants.SuspicionTrue && suspicion != constants.SuspecionFalse {
		fmt.Println("Invalid suspicion")
		return
	}
	node.switchmutex.Lock()
	//defer node.switchmutex.Unlock()
	node.protocol = protocol
	node.suspicion = suspicion
	fmt.Printf("NodeID %s switch protocol %s and suspicion %s\n", node.ID, node.protocol, node.suspicion)
	isGossip := false
	if node.protocol == constants.ProtocolGossip {
		isGossip = true
	}
	if isGossip {
		node.memberlist.resetOtherTime(node.ID)
	}
	node.switchmutex.Unlock()
	if isGossip {
		node.memberlist.addHeartBeat(node.ID)
		node.gossip()
	}
}

// MP3 test 1
func (node *Node) getHyDFSFile(filename string) string {
	return filepath.Join(node.storagePath, filename)
}

func (node *Node) getHyDFSLogFile(filename string) string {
	return filepath.Join(node.logPath, filename) + ".log"
}

// MP3 test 1
func (node *Node) FileInLocal(filename string) bool {
	filepath := node.getHyDFSFile(filename)
	_, err := os.Stat(filepath)
	if err == nil {
		return true
	} else {
		return false
	}
}

func (node *Node) Start() {
	go node.listenCommand()                   //mp2 command (port from constants.CommandPort)
	go node.listenMessage()                   // gossip udp
	go node.StartRPCServer(constants.RPCPort) // MP3 test 1
	//go node.CheckReplicaRoutine()
	if node.introducerAddr != "" {
		node.joinMemberList()
	} else {
		fmt.Println("This is the introducer")
	}
	go node.HeartBeatRoutine()
	go node.TimeOutCheck()
	// go node.CalculateBandwith()
}

// MP3 test 1
func (node *Node) StartRPCServer(rpcPort string) {
	err := rpc.Register(node)
	if err != nil {
		fmt.Println("Cannot register RPC")
		return
	}
	rpcAddr := node.host + ":" + constants.RPCPort
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		fmt.Println("RPC Server cannot listen through TCP")
		return
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Cannot accept the TCP connection")
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (node *Node) ReconcileFile(args *utils.ReconcileArgs, reply *utils.ReconcileReply) error {
	meta := node.getFileMetadata(args.HyDFSFileName)
	meta.lock.Lock()
	defer meta.lock.Unlock()
	err := node.localReconcileFile(args)
	if err != nil {
		reply.Success = false
		return err
	}
	reply.Success = true
	return nil
}

func (node *Node) localReconcileFile(args *utils.ReconcileArgs) error {
	filepath := node.getHyDFSFile(args.HyDFSFileName)
	currentFileData, err := os.ReadFile(filepath)
	if err == nil {
		currentHash := sha256.Sum256(currentFileData)
		if bytes.Equal(currentHash[:], args.FinalDataHash) {
			//fmt.Printf("[Reconcile] File %s is same. Skip\n", args.HyDFSFileName)
			logFilePath := node.getHyDFSLogFile(args.HyDFSFileName)
			err := os.WriteFile(logFilePath, args.FinalLogEntries, 0644)
			if err != nil {
				return err
			}
			return nil
		}
	}
	fmt.Printf("[Reconcile] %s (%s) File %s hash mismatch. Overwriting latest.\n", node.vmName, node.host, args.HyDFSFileName)
	err = os.WriteFile(filepath, args.FinalDataFile, 0644)
	if err != nil {
		return err
	}
	logFilePath := node.getHyDFSLogFile(args.HyDFSFileName)
	err = os.WriteFile(logFilePath, args.FinalLogEntries, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (node *Node) MergeFile(args *utils.MergeFileArgs, reply *utils.MergeFileReply) error {
	//fmt.Printf("[Merge] Primary %s starting merge for %s\n", node.vmName, args.HyDFSFileName)

	// 1. Get all replicas
	replicaInfo, err := utils.GetReplica(args.HyDFSFileName)
	if err != nil {
		reply.Success = false
		reply.ErrorMessage = "Cannot get replica info"
		return nil
	}

	meta := node.getFileMetadata(args.HyDFSFileName)
	meta.lock.Lock()
	defer meta.lock.Unlock()

	allOps := make(map[string]utils.OperationLogEntry)
	var allOpsMutex sync.Mutex

	var wg sync.WaitGroup
	selfRPCAddr := node.host + ":" + constants.RPCPort
	for _, replica := range replicaInfo {
		wg.Add(1)
		go func(replica utils.NodeInfo) {
			defer wg.Done()
			rpcAddr := replica.Address
			getLogArgs := &utils.GetLogArgs{
				HyDFSFileName: args.HyDFSFileName,
			}
			getLogReply := &utils.GetLogReply{}
			//fmt.Printf("[Merge] Polling log from %s\n", replica.Address)
			if selfRPCAddr == replica.Address {
				entries, localErr := node.GetLocalOperationLog(args.HyDFSFileName)
				if localErr == nil {
					getLogReply.Success = true
					getLogReply.Entries = entries
				}
			} else {
				err = utils.CallRPC(replica.Address, "Node.GetOperationLog", getLogArgs, getLogReply)
			}

			if err == nil && getLogReply.Success {
				//fmt.Printf("[Merge] Got %d log entries from %s\n", len(getLogReply.Entries), rpcAddr)
				allOpsMutex.Lock()
				for _, entry := range getLogReply.Entries {
					allOps[entry.OpID] = entry
				}
				allOpsMutex.Unlock()
			} else {
				otherVM, check := utils.GetVMName(rpcAddr)
				if !check {
					otherVM = rpcAddr
				}
				fmt.Printf("[Merge] FAILED to get %s log from %s (%s)\n", args.HyDFSFileName, otherVM, rpcAddr)
			}
		}(replica)
	}
	wg.Wait()

	finalLog := make([]utils.OperationLogEntry, 0, len(allOps))
	for _, entry := range allOps {
		finalLog = append(finalLog, entry)
	}
	sort.Slice(finalLog, func(i, j int) bool {
		return finalLog[i].OpID < finalLog[j].OpID
	})

	var dataBuffer bytes.Buffer
	var logBuffer bytes.Buffer

	for _, entry := range finalLog {
		dataBuffer.Write(entry.Data)
	}
	FinalDataFile := dataBuffer.Bytes()
	finalFileHash := sha256.Sum256(FinalDataFile)
	opID_merge := fmt.Sprintf("%d-%s", time.Now().UnixNano(), node.vmName)
	compactedEntry := utils.OperationLogEntry{
		OpID: opID_merge,
		Data: FinalDataFile,
	}
	logData, _ := json.Marshal(compactedEntry)
	logBuffer.Write(logData)
	logBuffer.WriteByte('\n')
	FinalLogEntries := logBuffer.Bytes()

	reconcileArgs := &utils.ReconcileArgs{
		HyDFSFileName:   args.HyDFSFileName,
		FinalDataFile:   FinalDataFile,
		FinalLogEntries: FinalLogEntries,
		FinalDataHash:   finalFileHash[:],
	}

	for _, replica := range replicaInfo {
		wg.Add(1)
		go func(replica utils.NodeInfo) {
			defer wg.Done()
			addr := replica.Address
			reconcileReply := &utils.ReconcileReply{}
			//fmt.Printf("[Merge] Pushing reconciled data to %s\n", addr)
			if addr == selfRPCAddr {
				err = node.localReconcileFile(reconcileArgs)
				if err == nil {
					reconcileReply.Success = true
				}
			} else {
				err = utils.CallRPC(addr, "Node.ReconcileFile", reconcileArgs, reconcileReply)
			}
			if err == nil && reconcileReply.Success {
				//fmt.Printf("[Merge] Reconcile Success for %s\n", addr)
			} else {
				otherVM, check := utils.GetVMName(addr)
				if !check {
					otherVM = addr
				}
				fmt.Printf("[Merge] Reconcile Failed for %s (%s)\n", otherVM, addr)
			}
		}(replica)
	}
	wg.Wait()

	//fmt.Printf("[Merge] Merge complete for %s.\n", args.HyDFSFileName)
	reply.Success = true
	return nil
}

// MP3 test 1
func (node *Node) CreateFile(args *utils.WriteFileArgs, reply *utils.WriteFileReply) error {
	fmt.Printf("%s (%s) received create request for %s\n", node.vmName, node.host, args.HyDFSFileName)
	vmlog.CreateLogger.Printf("%s received create request for %s bytes=%d", node.vmName, args.HyDFSFileName, len(args.LocalFile))

	// --- ADD LOCK --- Satisfies Test 5 Concurrency
	meta := node.getFileMetadata(args.HyDFSFileName)
	meta.lock.Lock()
	// --- END OF LOCK ADDITION ---

	logFilePath := node.getHyDFSLogFile(args.HyDFSFileName)
	if _, err := os.Stat(logFilePath); err == nil {
		fmt.Printf("File %s already exists\n", args.HyDFSFileName)
		reply.Success = false
		reply.ErrorMessage = "Error: File already exists."
		meta.lock.Unlock()
		return nil
	}
	isPrimary := (args.NonPrimary != nil && len(args.NonPrimary) > 0)
	filepath := node.getHyDFSFile(args.HyDFSFileName)
	err := os.WriteFile(filepath, args.LocalFile, 0644)
	if err != nil {
		fmt.Printf("Failed to write file %s\n", args.HyDFSFileName)
		reply.Success = false
		reply.ErrorMessage = err.Error()
		meta.lock.Unlock()
		return err
	}

	logEntry := utils.OperationLogEntry{
		OpID: args.OperationID,
		Data: args.LocalFile,
	}
	logData, _ := json.Marshal(logEntry)
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		os.Remove(filepath)
		meta.lock.Unlock()
		return err
	}

	_, err = logFile.Write(append(logData, '\n'))
	logFile.Close()
	if err != nil {
		os.Remove(filepath)
		os.Remove(logFilePath)
		meta.lock.Unlock()
		return err
	}
	meta.lock.Unlock()
	if isPrimary {
		fmt.Printf("[Create][FanOut] primary=%s file=%s targets=%v\n", node.host, args.HyDFSFileName, args.NonPrimary)
		vmlog.CreateLogger.Printf("%s fan-out create for %s to targets %v", node.vmName, args.HyDFSFileName, args.NonPrimary)
		err = node.primaryWrite("Node.CreateFile", args, reply)
		if err != nil {
			os.Remove(filepath)
			return nil
		}
	}
	fmt.Printf("%s (%s) finished create %s\n", node.vmName, node.host, args.HyDFSFileName)
	vmlog.CreateLogger.Printf("%s finished create %s", node.vmName, args.HyDFSFileName)
	reply.Success = true
	return nil
}

func (node *Node) getFileMetadata(filename string) *FileMetadata {
	node.metaLock.RLock()
	meta, ok := node.fileMetadata[filename]
	node.metaLock.RUnlock()

	if ok {
		return meta
	}

	node.metaLock.Lock()
	defer node.metaLock.Unlock()

	// Double-check in case another thread created it
	meta, ok = node.fileMetadata[filename]
	if ok {
		return meta
	}

	// Create it
	newMeta := &FileMetadata{}
	if node.fileMetadata == nil { // Ensure map is initialized
		node.fileMetadata = make(map[string]*FileMetadata)
	}
	node.fileMetadata[filename] = newMeta
	return newMeta
}

// AppendFile appends bytes to an existing HyDFS file on this node.
// The primary replica appends locally first, then synchronously
// replicates the same bytes to non-primary replicas via primaryWrite.
func (node *Node) AppendFile(args *utils.WriteFileArgs, reply *utils.WriteFileReply) error {
	fmt.Printf("[Append][Recv] vm=%s addr=%s file=%s bytes=%d\n", node.vmName, node.host, args.HyDFSFileName, len(args.LocalFile))
	vmlog.AppendLogger.Printf("%s received append request for %s bytes=%d", node.vmName, args.HyDFSFileName, len(args.LocalFile))

	// --- ADD THIS LOCK (Satisfies Test 5 Concurrency) ---
	meta := node.getFileMetadata(args.HyDFSFileName)
	meta.lock.Lock()
	// --- END OF LOCK ADDITION ---

	logFilePath := node.getHyDFSLogFile(args.HyDFSFileName)
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		fmt.Printf("File %s not found\n", args.HyDFSFileName)
		reply.Success = false
		reply.ErrorMessage = "Error: File not found."
		// meta.lock.Unlock()
		return nil
	}
	filepath := node.getHyDFSFile(args.HyDFSFileName)
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Cannot open file %s in AppendFile func\n", args.HyDFSFileName)
		// meta.lock.Unlock()
		return err
	}
	// Write the new bytes to the tail of the file.
	_, err = f.Write(args.LocalFile)
	if err != nil {
		fmt.Printf("Cannot write file %s in AppendFile func\n", args.HyDFSFileName)
		f.Close()
		meta.lock.Unlock()
		return err
	}
	f.Close()

	logEntry := utils.OperationLogEntry{
		OpID: args.OperationID,
		Data: args.LocalFile,
	}
	logData, _ := json.Marshal(logEntry)
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		meta.lock.Unlock()
		return err
	}

	_, err = logFile.Write(append(logData, '\n'))
	logFile.Close()
	if err != nil {
		meta.lock.Unlock()
		return err
	}
	meta.lock.Unlock()
	isPrimary := (args.NonPrimary != nil && len(args.NonPrimary) > 0)
	if isPrimary {
		// Fan-out the same append to all non-primary replicas in parallel.
		fmt.Printf("[Append][FanOut] primary=%s file=%s targets=%v\n", node.host, args.HyDFSFileName, args.NonPrimary)
		vmlog.AppendLogger.Printf("%s fan-out append for %s to targets %v", node.vmName, args.HyDFSFileName, args.NonPrimary)
		err = node.primaryWrite("Node.AppendFile", args, reply)
		if err != nil {
			return nil
		}
	}
	fmt.Printf("[Append][Done] vm=%s addr=%s file=%s\n", node.vmName, node.host, args.HyDFSFileName)
	vmlog.AppendLogger.Printf("%s finished append %s", node.vmName, args.HyDFSFileName)
	reply.Success = true
	return nil
}

// TriggerAppend is an RPC method that triggers an append operation on the target VM.
// The target VM reads the local file and then performs the append to the primary replica.
// This solves the "who has the file" problem - only the target VM can read its local file.
func (node *Node) TriggerAppend(args *utils.TriggerAppendArgs, reply *utils.TriggerAppendReply) error {
	fmt.Printf("[TriggerAppend][Recv] vm=%s localFile=%s hyDFSFile=%s\n", node.vmName, args.LocalFileName, args.HyDFSFileName)

	// Step 1: Read the local file (we are on the target VM, so this works)
	localFileData, err := os.ReadFile(args.LocalFileName)
	if err != nil {
		reply.Success = false
		reply.ErrorMessage = fmt.Sprintf("Cannot read local file %s: %v", args.LocalFileName, err)
		return nil
	}

	// Step 2: Get replica info to find primary
	replicaInfo, err := utils.GetReplica(args.HyDFSFileName)
	if err != nil {
		reply.Success = false
		reply.ErrorMessage = fmt.Sprintf("Cannot get replica info: %v", err)
		return nil
	}

	// Step 3: Find primary and build non-primary list
	primaryIndex := rand.Intn(constants.ReplicaNum)
	primary := replicaInfo[primaryIndex]
	nonPrimary := []string{}
	for i := 0; i < constants.ReplicaNum; i++ {
		if i == primaryIndex {
			continue
		}
		nonPrimary = append(nonPrimary, replicaInfo[i].Address)
	}
	opID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), node.vmName)
	// Step 4: Prepare WriteFileArgs with the actual file data
	writeArgs := &utils.WriteFileArgs{
		HyDFSFileName: args.HyDFSFileName,
		LocalFile:     localFileData,
		NonPrimary:    nonPrimary,
		OperationID:   opID,
	}

	fmt.Printf("[TriggerAppend][Send] vm=%s primary=%s file=%s local=%s bytes=%d\n", node.vmName, primary.Address, args.HyDFSFileName, args.LocalFileName, len(localFileData))

	// Step 5: Call AppendFile RPC on the primary
	writeReply := &utils.WriteFileReply{}
	err = utils.CallRPC(primary.Address, "Node.AppendFile", writeArgs, writeReply)
	if err != nil {
		reply.Success = false
		reply.ErrorMessage = fmt.Sprintf("Cannot call RPC AppendFile on primary: %v", err)
		return nil
	}

	if writeReply.Success {
		fmt.Printf("[TriggerAppend][Done] vm=%s file=%s\n", node.vmName, args.HyDFSFileName)
		reply.Success = true
	} else {
		reply.Success = false
		reply.ErrorMessage = writeReply.ErrorMessage
	}

	return nil
}

func (node *Node) GetLocalOperationLog(hyDFSFileName string) ([]utils.OperationLogEntry, error) {
	logFilePath := node.getHyDFSLogFile(hyDFSFileName)
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		fmt.Printf("[GetLog] %s (%s) log file not found for %s\n", node.vmName, node.host, hyDFSFileName)
		return nil, err
	}

	file, _ := os.Open(logFilePath)
	defer file.Close()
	var entries []utils.OperationLogEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry utils.OperationLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err == nil {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func (node *Node) GetOperationLog(args *utils.GetLogArgs, reply *utils.GetLogReply) error {
	meta := node.getFileMetadata(args.HyDFSFileName)
	meta.lock.Lock()
	defer meta.lock.Unlock()
	entries, err := node.GetLocalOperationLog(args.HyDFSFileName)
	if err != nil {
		reply.Success = false
		return nil
	}

	reply.Entries = entries
	reply.Success = true
	//fmt.Printf("[GetLog] Node %s (%s) is returning entries for %s\n", node.vmName, node.host, args.HyDFSFileName)
	return nil
}

// primaryWrite coordinates replication from the primary replica to all non-primary replicas.
// It implements all-or-nothing semantics: the operation only succeeds if ALL non-primary
// replicas successfully complete the write operation.
func (node *Node) primaryWrite(method string, args *utils.WriteFileArgs, reply *utils.WriteFileReply) error {
	nonPrimaryArgs := &utils.WriteFileArgs{
		HyDFSFileName: args.HyDFSFileName,
		LocalFile:     args.LocalFile,
		NonPrimary:    nil, // This will ensure that the non-primary replicas do not replicate further
		OperationID:   args.OperationID,
	}
	var wg sync.WaitGroup
	var createCountMu sync.Mutex
	createCount := 0
	// Fan-out: send parallel RPC calls to each non-primary replica
	for _, nonPrimaryAddr := range args.NonPrimary {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var nonPrimaryReply utils.WriteFileReply
			err := utils.CallRPC(addr, method, nonPrimaryArgs, &nonPrimaryReply)
			if err == nil && nonPrimaryReply.Success {
				createCountMu.Lock()
				createCount++
				createCountMu.Unlock()
				fmt.Printf("[Replicate][OK] file=%s target=%s\n", args.HyDFSFileName, addr)
				if method == "Node.CreateFile" {
					vmlog.CreateLogger.Printf("replicate create for %s to %s succeeded", args.HyDFSFileName, addr)
				} else if method == "Node.AppendFile" {
					vmlog.AppendLogger.Printf("replicate append for %s to %s succeeded", args.HyDFSFileName, addr)
				}
			} else {
				fmt.Printf("[Replicate][FAIL] file=%s target=%s method=%s err=%v\n", args.HyDFSFileName, addr, method, err)
				if method == "Node.CreateFile" {
					vmlog.CreateLogger.Printf("replicate create for %s to %s failed: %v", args.HyDFSFileName, addr, err)
				} else if method == "Node.AppendFile" {
					vmlog.AppendLogger.Printf("replicate append for %s to %s failed: %v", args.HyDFSFileName, addr, err)
				}
			}
		}(nonPrimaryAddr)
	}
	// Wait for all parallel replication calls to complete
	wg.Wait()
	// All-or-nothing check: if any replica failed, the entire operation fails
	if createCount != len(args.NonPrimary) {
		reply.Success = false
		reply.ErrorMessage = "Failed to replicate all non primary"
		return fmt.Errorf("Failed to replicate all non primary for method %s\n", method)
	}
	return nil
}

// MP3 Test 2
func (node *Node) GetFile(args *utils.GetFileArgs, reply *utils.GetFileReply) error {
	fmt.Printf("%s (%s) received get request for %s\n", node.vmName, node.host, args.HyDFSFileName)
	vmlog.GetLogger.Printf("%s received get request for %s", node.vmName, args.HyDFSFileName)

	meta := node.getFileMetadata(args.HyDFSFileName)
	meta.lock.Lock()
	defer meta.lock.Unlock()

	if !node.FileInLocal(args.HyDFSFileName) {
		fmt.Printf("File %s not found\n", args.HyDFSFileName)
		reply.Success = false
		reply.ErrorMessage = "Error: File not found."
		return nil
	}
	filepath := node.getHyDFSFile(args.HyDFSFileName)
	fileData, err := os.ReadFile(filepath)
	if err != nil {
		fmt.Printf("Failed to read file %s\n", args.HyDFSFileName)
		reply.Success = false
		reply.ErrorMessage = err.Error()
		return err
	}
	reply.FileData = fileData
	reply.Success = true
	fmt.Printf("%s (%s) finished get %s\n", node.vmName, node.host, args.HyDFSFileName)
	vmlog.GetLogger.Printf("%s finished get %s bytes=%d", node.vmName, args.HyDFSFileName, len(fileData))
	return nil
}

// MP3 Test 3
func (node *Node) CheckFile(args *utils.CheckFileArgs, reply *utils.CheckFileReply) error {
	meta := node.getFileMetadata(args.HyDFSFileName)
	meta.lock.Lock()
	filepath := node.getHyDFSFile(args.HyDFSFileName)
	currentData, err := os.ReadFile(filepath)
	meta.lock.Unlock()
	if err != nil {
		reply.Success = false
		reply.ErrorMessage = err.Error()
		return nil
	}
	currentHash := sha256.Sum256(currentData)
	reply.Success = true
	reply.CheckHash = currentHash[:]
	return nil
}

func (node *Node) TimeOutCheck() {
	ticker := time.NewTicker(constants.ProtocolPeriod)
	defer ticker.Stop()
	for range ticker.C {
		node.switchmutex.Lock()
		protocol := node.protocol
		suspicion := node.suspicion
		node.switchmutex.Unlock()
		failedMemberList := node.memberlist.TimeOutCheck(protocol, suspicion)
		if len(failedMemberList) > 0 {
			node.gossipUPdateMember(failedMemberList)
		}
	}
}

func (node *Node) CheckReplicaRoutine() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		node.checkReplica()
	}
}

func (node *Node) checkReplica() {
	rpcAddr := node.host + ":" + constants.RPCPort
	currentMembers := node.memberlist.getMemberList()
	ringNodes := utils.GetRingInfo(currentMembers)

	sort.Slice(ringNodes, func(i, j int) bool {
		return ringNodes[i].NodeID < ringNodes[j].NodeID
	})

	hyDFSFiles, err := os.ReadDir(node.storagePath)
	if err != nil {
		return
	}
	for _, hyDFSFile := range hyDFSFiles {
		hyDFSFileName := hyDFSFile.Name()
		fileID := utils.HashFunc(hyDFSFileName)
		correctReplica := utils.GetSuccessor(fileID, ringNodes, constants.ReplicaNum)
		selfReplica := false
		selfPrimary := false
		if rpcAddr == correctReplica[0].Address {
			selfPrimary = true
		}
		for _, replica := range correctReplica {
			if rpcAddr == replica.Address {
				selfReplica = true
				break
			}
		}
		if selfReplica {
			if selfPrimary {
				go func(filename string) {
					//fmt.Printf("[CheckReplica] background merge for %s\n", filename)
					mergeArgs := &utils.MergeFileArgs{HyDFSFileName: filename}
					mergeReply := &utils.MergeFileReply{}
					err := node.MergeFile(mergeArgs, mergeReply)
					if err != nil || !mergeReply.Success {
						fmt.Printf("[CheckReplica][Failed] primary %s background merge for %s: %v\n", node.vmName, filename, err)
					} else {
						//fmt.Printf("[CheckReplica][DONE] primary %s background merge %s\n", node.vmName, filename)
					}
				}(hyDFSFileName)
				// go node.Rereplicate(hyDFSFileName, correctReplica[1:])
			}
		} else {
			fmt.Printf("%s (%s) no longer a replica for %s (fileID: %d)\n", node.vmName, node.host, hyDFSFileName, fileID)
			os.Remove(node.getHyDFSFile(hyDFSFileName))
			os.Remove(node.getHyDFSLogFile(hyDFSFileName))
		}
	}
}

func (node *Node) Rereplicate(filename string, nonPrimary []utils.NodeInfo) {
	meta := node.getFileMetadata(filename)
	meta.lock.Lock()
	filepath := node.getHyDFSFile(filename)
	logFilePath := node.getHyDFSLogFile(filename)
	dataFile, errData := os.ReadFile(filepath)
	logFile, errLog := os.ReadFile(logFilePath)
	meta.lock.Unlock()

	if errData != nil || errLog != nil {
		fmt.Printf("[Rereplicate] Faied: Primary %s cannot read filename %s\n", node.vmName, filename)
		return
	}
	fileHash := sha256.Sum256(dataFile)
	var wg sync.WaitGroup
	for _, otherReplica := range nonPrimary {
		wg.Add(1)
		go func(replica utils.NodeInfo) {
			defer wg.Done()
			rpcAddr := replica.Address
			checkArgs := &utils.CheckFileArgs{
				HyDFSFileName: filename,
			}
			checkReply := &utils.CheckFileReply{}
			err := utils.CallRPC(rpcAddr, "Node.CheckFile", checkArgs, checkReply)
			if err != nil || !checkReply.Success || !bytes.Equal(checkReply.CheckHash, fileHash[:]) {
				reconcileArgs := &utils.ReconcileArgs{
					HyDFSFileName:   filename,
					FinalDataFile:   dataFile,
					FinalLogEntries: logFile,
					FinalDataHash:   fileHash[:],
				}
				reconcileReply := &utils.ReconcileReply{}
				otherHost, _, _ := net.SplitHostPort(rpcAddr)
				otherVMName, check := utils.IpToVMName[otherHost]
				if !check {
					otherVMName = rpcAddr
				}
				fmt.Printf("%s (%s) \n [Rereplicate] file %s to %s (%s)\n", node.vmName, node.host, filename, otherVMName, otherHost)
				err := utils.CallRPC(rpcAddr, "Node.ReconcileFile", reconcileArgs, reconcileReply)
				if err != nil || !reconcileReply.Success {
					fmt.Printf("[Rereplicate] Error: %v failed to Re-Replicate %s to %s (%s)\n", err, filename, otherVMName, rpcAddr)
				} else {
					fmt.Printf("[Rereplicate] [DONE] %s to %s (%s)\n", filename, otherVMName, rpcAddr)
				}
			}
		}(otherReplica)
	}
	wg.Wait()
}

func (node *Node) listenCommand() {
	tcpaddr := ":" + node.commandPort
	ln, err := net.Listen("tcp", tcpaddr)
	if err != nil {
		fmt.Println("Cannot start listening")
		return
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Cannot accept the TCP connection")
			continue
		}
		go node.HandleCommand(conn)
	}
}

func (node *Node) HandleCommand(conn net.Conn) {
	defer conn.Close()
	bufReader := bufio.NewReader(conn)
	concatCommand, err := bufReader.ReadString('\n')
	if err != nil {
		fmt.Println("Cannot read through TCP connection")
		return
	}
	concatCommand = strings.TrimSuffix(concatCommand, "\n")
	commandArgs := strings.Split(concatCommand, " ")
	if len(commandArgs) > 0 {
		switch commandArgs[0] {
		case "switch":
			if len(commandArgs) != 3 {
				fmt.Println("Invalid switch format")
				return
			}
			protocol := commandArgs[1]
			suspicion := commandArgs[2]
			node.Switch(protocol, suspicion)
			if node.introducerAddr == "" {
				node.SendSwitchCommand(protocol, suspicion)
			}
		case "get_switch":
			node.switchmutex.Lock()
			protocol := node.protocol
			suspicion := node.suspicion
			node.switchmutex.Unlock()
			sendMode := fmt.Sprintf("mode %s %s\n", protocol, suspicion)
			_, err := conn.Write([]byte(sendMode))
			if err != nil {
				fmt.Println("Introducer: cannot write switch mode to other members")
			}
		case "list_mem":
			memberListAll := node.memberlist.printMemberList()
			_, err := conn.Write([]byte(memberListAll))
			if err != nil {
				fmt.Println("Cannot get the memberlist")
			}
		case "display_protocol":
			currentProtocol := node.GetMode()
			_, err := conn.Write([]byte(currentProtocol))
			if err != nil {
				fmt.Println("Cannot get the <protocol ,suspicion>")
			}
		case "display_suspects":
			currentSuspect := node.memberlist.listSuspect()
			_, err := conn.Write([]byte(currentSuspect))
			if err != nil {
				fmt.Println("Cannot get current suspected nodes")
			}
		case "list_self":
			currentid := node.GetID()
			_, err := conn.Write([]byte(currentid))
			if err != nil {
				fmt.Println("Cannot get the id")
			}
		case "grep":
			grepArg := commandArgs[1:]
			node.grepConnect2Server(conn, grepArg)
		case "mem_list_json":
			memberlist := node.memberlist.getMemberList()
			jsonMember, err := json.Marshal(memberlist)
			if err != nil {
				conn.Write([]byte("Cannot Marshal memberlist\n"))
				return
			}
			conn.Write(jsonMember)
		case "leave":
			go node.memberlist.TurnLeave(node.ID)
			conn.Write([]byte("Leavning now,\n"))
		case "liststore":
			ringID := utils.HashFunc(node.ID)
			files, err := os.ReadDir(node.storagePath)
			if err != nil {
				errorMessage := "Cannot read hydfs directory\n"
				conn.Write([]byte(errorMessage))
				return
			}
			var responses bytes.Buffer
			idInfo := fmt.Sprintf("Files stored at RingID %d\n--------------------\n", ringID)
			responses.WriteString(idInfo)
			for _, file := range files {
				filename := file.Name()
				fileID := utils.HashFunc(filename)
				fileInfo := fmt.Sprintf("filename %s fileID %d\n", filename, fileID)
				responses.WriteString(fileInfo)
			}
			conn.Write(responses.Bytes())
		}

	}
}

func (node *Node) GetMode() string {
	node.switchmutex.Lock()
	protocol := node.protocol
	suspicion := node.suspicion
	node.switchmutex.Unlock()
	return "<" + protocol + ", " + suspicion + ">\n"
}

func (node *Node) SendSwitchCommand(protocol string, suspicion string) {
	allMembers := node.memberlist.getMemberList()
	commandString := fmt.Sprintf("switch %s %s\n", protocol, suspicion)
	for _, eachMember := range allMembers {
		if eachMember.ID == node.ID {
			continue
		}
		host := strings.Split(eachMember.Address, ":")[0]
		tcpAddr := fmt.Sprintf("%s:%s", host, constants.CommandPort)
		go func(address string) {
			conn, err := net.DialTimeout("tcp", address, 1*time.Second)
			if err != nil {
				fmt.Println("Introducer cannot send protocol and suspicion.")
			}
			defer conn.Close()
			_, err = conn.Write([]byte(commandString))
			if err != nil {
				fmt.Println("Cannot write switch command through TCP command")
			}
		}(tcpAddr)
	}
}

func (node *Node) joinMemberList() {
	nodeitself, OK := node.memberlist.getSelfByID(node.ID)
	if !OK {
		fmt.Printf("the ID itself not in its member list")
		return
	}
	joinMessage := message.Message{
		Type:       constants.MessageJoin,
		Memberlist: []member.Member{nodeitself},
		SendID:     node.ID,
	}
	node.nodeSendMessage(joinMessage, node.introducerAddr)
	go func() {
		introducerHost := strings.Split(node.introducerAddr, ":")[0]
		tcpAddr := fmt.Sprintf("%s:%s", introducerHost, constants.CommandPort)
		conn, err := net.DialTimeout("tcp", tcpAddr, 2*time.Second)
		if err != nil {
			fmt.Println("Timeout for getting mode from introducer")
			return
		}
		defer conn.Close()
		_, err = conn.Write([]byte("get_switch\n"))
		if err != nil {
			fmt.Println("Cannot write get_switch to introducer")
		}
		bufReader := bufio.NewReader(conn)
		modeString, err := bufReader.ReadString('\n')
		if err != nil {
			fmt.Println("Cannot read through TCP connection")
			return
		}
		modeString = strings.TrimSuffix(modeString, "\n")
		commandArgs := strings.Split(modeString, " ")
		if len(commandArgs) == 3 && commandArgs[0] == "mode" {
			protocol := commandArgs[1]
			suspicion := commandArgs[2]
			node.Switch(protocol, suspicion)
		}
	}()
}

func (node *Node) listenMessage() {
	buffer := make([]byte, constants.ListenBufSize)
	for {
		bufsize, sender, err := node.udpconn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error when reading from listen message")
			continue
		}
		node.bandwithMutex.Lock()
		node.totalBytes += int64(bufsize)
		node.bandwithMutex.Unlock()
		var message message.Message
		if err := json.Unmarshal(buffer[:bufsize], &message); err != nil {
			fmt.Println("Cannot extract the message")
			continue
		}
		go node.handleMessage(message, sender)
	}
}

func (node *Node) HeartBeatRoutine() {
	for {
		node.switchmutex.Lock()
		nowProtocol := node.protocol
		node.switchmutex.Unlock()
		if nowProtocol == constants.ProtocolGossip {
			node.memberlist.addHeartBeat(node.ID)
			node.gossip()
		} else if nowProtocol == constants.ProtocolPingAck {
			node.pingMember()
		}
		var timePeriod time.Duration
		if nowProtocol == constants.ProtocolGossip {
			timePeriod = constants.ProtocolPeriod
		} else if nowProtocol == constants.ProtocolPingAck {
			timePeriod = constants.PingFrequency
		}
		time.Sleep(timePeriod)
	}
}

func (node *Node) pingMember() {
	selectedMember := node.memberlist.roundRobinK(1, node.ID)
	if len(selectedMember) > 0 {
		targetMember := selectedMember[0]
		node.switchmutex.Lock()
		nowSuspicion := node.suspicion
		node.switchmutex.Unlock()
		go func(targetMember member.Member) {
			newAckChan := make(chan bool, 1)
			node.ackMutex.Lock()
			node.ackchan[targetMember.ID] = newAckChan
			node.ackMutex.Unlock()
			defer func() {
				node.ackMutex.Lock()
				delete(node.ackchan, targetMember.ID)
				node.ackMutex.Unlock()
			}()
			sendMessage := message.Message{
				Type:       constants.MessagePing,
				Memberlist: node.memberlist.getMemberList(),
				SendID:     node.ID,
			}
			node.nodeSendMessage(sendMessage, targetMember.Address)
			select {
			case <-newAckChan:
				if nowSuspicion == constants.SuspicionTrue {
				}
			case <-time.After(constants.PingTimeout):
				if nowSuspicion == constants.SuspicionTrue {
					updateMember := node.memberlist.changeState(targetMember.ID, constants.Alive, constants.Suspected)
					//Not sure about that
					node.gossipUPdateMember(updateMember)
				} else if nowSuspicion == constants.SuspecionFalse {
					updateMember := node.memberlist.changeState(targetMember.ID, constants.Alive, constants.Failed)
					//Not sure about that
					node.gossipUPdateMember(updateMember)
				}
			}
		}(*targetMember)
	}
}

func (node *Node) sendGossipMessage(memberlist []member.Member) {
	if len(memberlist) == 0 {
		return
	}
	targetMembers := node.memberlist.roundRobinK(constants.GossipNum, node.ID)
	if len(targetMembers) == 0 {
		return
	}
	updateMemberMessage := message.Message{
		Type:       constants.MessageGossip,
		Memberlist: memberlist,
		SendID:     node.ID,
	}
	for _, targetmember := range targetMembers {
		node.nodeSendMessage(updateMemberMessage, targetmember.Address)
	}
}

func (node *Node) gossip() {
	node.sendGossipMessage(node.memberlist.getMemberList())
}

func (node *Node) gossipUPdateMember(member []member.Member) {
	node.sendGossipMessage(member)
}

func (node *Node) handleMessage(msg message.Message, sender *net.UDPAddr) {
	randomNumber := rand.Float64()
	if randomNumber < constants.DropRate {
		return
	}
	node.memberlist.updateSenderTime(node.ID, msg.SendID)
	node.memberlist.UpdateMemberlist(msg.Memberlist, node.ID)
	switch msg.Type {
	case constants.MessagePing:
		returnMessage := message.Message{
			Type:       constants.MessageAck,
			Memberlist: node.memberlist.getMemberList(),
			SendID:     node.ID,
		}
		node.nodeSendMessage(returnMessage, sender.String())
	case constants.MessageAck:
		node.ackMutex.Lock()
		ackChan, OK := node.ackchan[msg.SendID]
		if OK {
			ackChan <- true
		}
		node.ackMutex.Unlock()
	case constants.MessageGossip:
		break
	case constants.MessageJoin:
		returnMessage := message.Message{
			Type:       constants.MessageGossip,
			Memberlist: node.memberlist.getMemberList(),
			SendID:     node.ID,
		}
		node.nodeSendMessage(returnMessage, sender.String())
	}
}

func (node *Node) nodeSendMessage(message message.Message, addressStr string) {
	udpAddress, err := net.ResolveUDPAddr("udp", addressStr)
	if err != nil {
		fmt.Println("Cannot get the udpaddress")
		return
	}
	bytes, err := json.Marshal(message)
	if err != nil {
		fmt.Println("Cannot encode the json message")
		return
	}
	byteswrite, err := node.udpconn.WriteToUDP(bytes, udpAddress)
	if err != nil {
		fmt.Println("Cannot write message through UDP")
		return
	}
	node.bandwithMutex.Lock()
	node.totalBytes += int64(byteswrite)
	node.bandwithMutex.Unlock()
}

func (node *Node) GetID() string {
	return node.ID + "\n"
}

func (node *Node) GetMemberList() []member.Member {
	return node.memberlist.getMemberList()
}

func (node *Node) GetProtocol() string {
	return node.protocol
}

func (node *Node) GetSuspicion() string {
	return node.suspicion
}

func (node *Node) GetLogFile() string {

	parts := strings.Split(node.ID, ":")
	if len(parts) >= 2 {
		return fmt.Sprintf("machine.%s.log", parts[1])
	}
	return "machine.unknown.log"
}

func (node *Node) grepConnect2Server(conn net.Conn, commandArgs []string) {
	machineLogfile := node.GetLogFile()
	commandArgs = append(commandArgs, machineLogfile)
	cmd := exec.Command("grep", commandArgs...)
	grepOutut, err := cmd.Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 {
				// Grep returns exit code 1 when no matches found, but may still have output (e.g., "0" with -c flag)
				// Write the output even if exit code is 1
				if len(grepOutut) > 0 {
					_, writeErr := conn.Write(grepOutut)
					if writeErr != nil {
						fmt.Println("Cannot write grep result to the client")
					}
				}
				return
			} else {
				fmt.Println("Grep execution error")
			}
		} else {
			fmt.Println("Grep error other than execution")
		}
		return
	}
	_, writeErr := conn.Write(grepOutut)
	if writeErr != nil {
		fmt.Println("Cannot write grep result to the client")
	}
}

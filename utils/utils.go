package utils

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"

	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/constants"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/member"
)

type NodeInfo struct {
	NodeID  uint32
	Address string
}

type WriteFileArgs struct {
	HyDFSFileName string
	LocalFile     []byte
	NonPrimary    []string
	OperationID   string
}

type WriteFileReply struct {
	Success      bool
	ErrorMessage string
}

type GetFileArgs struct {
	HyDFSFileName string
}

type GetFileReply struct {
	Success      bool
	ErrorMessage string
	FileData     []byte
}

type CheckFileArgs struct {
	HyDFSFileName string
}

type CheckFileReply struct {
	Success      bool
	ErrorMessage string
	CheckHash    []byte
}

// MergeFileArgs is the payload for the client's MergeFile RPC
type MergeFileArgs struct {
	HyDFSFileName string
}

// MergeFileReply is the response from the MergeFile RPC
type MergeFileReply struct {
	Success      bool
	ErrorMessage string
}

// TriggerAppendArgs is the payload for the TriggerAppend RPC
// This is sent from the initiator VM to the target VM to trigger an append
type TriggerAppendArgs struct {
	LocalFileName string // The local file path on the target VM
	HyDFSFileName string // The HyDFS file to append to
}

// TriggerAppendReply is the response from the TriggerAppend RPC
type TriggerAppendReply struct {
	Success      bool
	ErrorMessage string
}

type OperationLogEntry struct {
	OpID string `json:"op_id"`
	Data []byte `json:"data"`
}

type ReconcileArgs struct {
	HyDFSFileName   string
	FinalDataFile   []byte
	FinalLogEntries []byte
	FinalDataHash   []byte
}

type ReconcileReply struct {
	Success bool
}

type GetLogArgs struct {
	HyDFSFileName string
}

type GetLogReply struct {
	Success bool
	Entries []OperationLogEntry
}

func HashFunc(key string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return hash.Sum32()
}

var IpToVMName = map[string]string{
	"172.22.155.16": "VM1",
	"172.22.159.16": "VM2",
	"172.22.95.202": "VM3",
	"172.22.155.17": "VM4",
	"172.22.159.17": "VM5",
	"172.22.95.203": "VM6",
	"172.22.155.18": "VM7",
	"172.22.159.18": "VM8",
	"172.22.95.204": "VM9",
	"172.22.155.19": "VM10",
}

var vmNameToIP = map[string]string{
	"VM1":  "172.22.155.16",
	"VM2":  "172.22.159.16",
	"VM3":  "172.22.95.202",
	"VM4":  "172.22.155.17",
	"VM5":  "172.22.159.17",
	"VM6":  "172.22.95.203",
	"VM7":  "172.22.155.18",
	"VM8":  "172.22.159.18",
	"VM9":  "172.22.95.204",
	"VM10": "172.22.155.19",
}

// GetVMHost finds the IP address for a given VM name (e.g., "VM1" -> "172.22.155.16")
// ExecMultiAppend uses this to know where to send its commands.
func GetVMHost(vmName string) (string, bool) {
	// Standardize on uppercase "VM1", "VM2", etc.
	name := strings.ToUpper(vmName)
	ip, ok := vmNameToIP[name]
	return ip, ok
}

// Given the address, map with correct VM name
func GetVMName(addr string) (string, bool) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Printf("Cannot extract the host through udp address: %s\n", addr)
		return "", false
	}
	vmName, ok := IpToVMName[host]
	if !ok {
		fmt.Printf("Cannot find in existing VM[1-10]\n")
		return "", false
	}
	return vmName, true
}

// VMNameToIP resolves a VM name (e.g., "VM2" or "vm2") to its IP using IpToVMName.
// The lookup is case-insensitive and accepts formats like "vm10".
func VMNameToIP(vmName string) (string, bool) {
	normalized := strings.ToUpper(vmName)
	// ensure it starts with VM
	if !strings.HasPrefix(normalized, "VM") {
		normalized = "VM" + normalized
	}
	for ip, name := range IpToVMName {
		if strings.EqualFold(name, normalized) {
			return ip, true
		}
	}
	return "", false
}

// RPC Method for dialing address and handling closing the connection.
func CallRPC(address string, method string, args interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Call(method, args, reply)
}

// Convert the udp address to rpc address
func Gossip2RPCAddr(gossipAddr string) (string, error) {
	lastColon := strings.LastIndex(gossipAddr, ":")
	if lastColon == -1 {
		return "", fmt.Errorf("Invalid gossip address %s\n", gossipAddr)
	}
	host := gossipAddr[:lastColon]
	return net.JoinHostPort(host, constants.RPCPort), nil
}

// Convert the memberlist to ringID information for all VMs
func GetRingInfo(currentMembers []member.Member) []NodeInfo {
	leaderIP, _ := VMNameToIP("VM1")
	leaderaddr := leaderIP + ":" + constants.RPCPort
	ringNodes := make([]NodeInfo, 0, len(currentMembers))
	for _, member := range currentMembers {
		rpcAddr, err := Gossip2RPCAddr(member.Address)
		if rpcAddr == leaderaddr {
			// skip leader as replica node
			continue
		}
		if err != nil {
			fmt.Printf("Cannot convert gossip address %s to rpc address", member.Address)
			continue
		}
		ringNodes = append(ringNodes, NodeInfo{
			NodeID:  HashFunc(member.ID),
			Address: rpcAddr,
		})
	}
	return ringNodes
}

// Given the fileID (after hashing), sorted ringID for all VMs, and replica factor
// return the successor
func GetSuccessor(fileID uint32, nodeInfo []NodeInfo, num int) []NodeInfo {
	successor := make([]NodeInfo, 0, num)
	startIndex := 0
	for i, node := range nodeInfo {
		if node.NodeID >= fileID {
			startIndex = i
			break
		}
	}
	for i := 0; i < num; i++ {
		index := (startIndex + i) % len(nodeInfo)
		successor = append(successor, nodeInfo[index])
	}
	return successor
}

// Given the hyDFSFileName, return 3 replicas that should store the file
func GetReplica(hyDFSFileName string) ([]NodeInfo, error) {
	fileID := HashFunc(hyDFSFileName)
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("Cannot get hostname")
	}
	address := host + ":" + constants.CommandPort
	memberListJSON, err := SendCommand(address, "mem_list_json")
	if err != nil {
		return nil, fmt.Errorf("Cannot get memberlist")
	}
	var currentMembers []member.Member
	err = json.Unmarshal([]byte(memberListJSON), &currentMembers)
	if err != nil {
		return nil, fmt.Errorf("Cannot Marshal the memberlist")
	}

	if len(currentMembers) < constants.ReplicaNum {
		return nil, fmt.Errorf("Not enough node for replica")
	}

	ringNodes := GetRingInfo(currentMembers)

	if len(ringNodes) < constants.ReplicaNum {
		return nil, fmt.Errorf("Not enough valid node for replica")
	}

	sort.Slice(ringNodes, func(i, j int) bool {
		return ringNodes[i].NodeID < ringNodes[j].NodeID
	})

	replicaInfo := GetSuccessor(fileID, ringNodes, constants.ReplicaNum)
	return replicaInfo, nil
}

// MP2 TCP connection to get node information such as memberlist, protocol
func SendCommand(addr string, command string) (string, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if !strings.HasSuffix(command, "\n") {
		command += "\n"
	}
	_, err = conn.Write([]byte(command))
	if err != nil {
		fmt.Println("Cannot write command through TCP connection")
		return "", err
	}
	cliResponse, err := io.ReadAll(conn)
	if err != nil {
		return "", err
	}
	return string(cliResponse), nil
}

func GetSelfVM() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		hostname, _ := os.Hostname()
		return hostname
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip := localAddr.IP.String()
	if vmName, ok := IpToVMName[ip]; ok {
		return vmName
	}
	return ip
}

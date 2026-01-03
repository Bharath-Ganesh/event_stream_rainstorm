package server

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/constants"
	. "gitlab.engr.illinois.edu/yonghan4/mp4_g82/distributed_log_querier/utils"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/member"
)

// Global node instance - will be set by the main program
var globalNode interface {
	GetMemberList() []member.Member
	GetID() string
	GetProtocol() string
	GetSuspicion() string
	GetLogFile() string
	Switch(protocol string, suspicion string)
}

// SetNode sets the global node instance
func SetNode(node interface {
	GetMemberList() []member.Member
	GetID() string
	GetProtocol() string
	GetSuspicion() string
	GetLogFile() string
	Switch(protocol string, suspicion string)
}) {
	globalNode = node
}

func Connect2Server(conn net.Conn) {
	defer conn.Close()
	bufReader := bufio.NewReader(conn)
	concatCommand, err := bufReader.ReadString('\n')
	if err != nil {
		if err.Error() == "EOF" {
			return
		}
		CheckError(err, "Cannot read from the client")
	}
	concatCommand = strings.TrimSuffix(concatCommand, "\n")
	commandArgs := strings.Split(concatCommand, " ")

	if len(commandArgs) == 0 {
		conn.Write([]byte("Error: Empty command\n"))
		return
	}

	command := commandArgs[0]
	args := commandArgs[1:]

	switch command {
	case "grep":
		handleGrepCommand(conn, args)
	case "list_mem":
		handleListMemCommand(conn)
	case "list_self":
		handleListSelfCommand(conn)
	case "display_suspects":
		handleDisplaySuspectsCommand(conn)
	case "display_protocol":
		handleDisplayProtocolCommand(conn)
	case "switch":
		handleSwitchCommand(conn, args)
	default:
		conn.Write([]byte("Error: Unknown command. Available: grep, list_mem, list_self, display_suspects, display_protocol, switch, append\n"))
	}
}

func handleGrepCommand(conn net.Conn, args []string) {
	if globalNode == nil {
		conn.Write([]byte("Error: Node not available\n"))
		return
	}

	logFile := globalNode.GetLogFile()
	grepArgs := []string{}
	grepArgs = append(grepArgs, args...)
	grepArgs = append(grepArgs, logFile)
	out, err := exec.Command("grep", grepArgs...).Output()
	// might not find matching error, cannot use utils checkError directly
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 {
				// No matching results
				conn.Write([]byte("No matching results\n"))
			} else {
				conn.Write([]byte("Grep execution error\n"))
			}
		} else {
			conn.Write([]byte("Grep error other than execution\n"))
		}
		return
	}
	_, writeErr := conn.Write(out)
	if writeErr != nil {
		fmt.Println("Cannot write to the client")
	}
}

func handleListMemCommand(conn net.Conn) {
	if globalNode == nil {
		conn.Write([]byte("Error: Node not available\n"))
		return
	}

	members := globalNode.GetMemberList()
	output := "Membership List:\n"
	output += "================\n"

	if len(members) == 0 {
		output += "No members in the list\n"
	} else {
		for _, member := range members {
			output += fmt.Sprintf("ID: %s\n", member.ID)
			output += fmt.Sprintf("Address: %s\n", member.Address)
			output += fmt.Sprintf("State: %s\n", member.State)
			output += fmt.Sprintf("Heartbeat: %d\n", member.HeartBeatCounter)
			output += fmt.Sprintf("Incarnation: %d\n", member.Incarnation)
			output += fmt.Sprintf("Last Update: %s\n", member.LastUpdateTime.Format(time.RFC3339))
			output += "---\n"
		}
	}

	conn.Write([]byte(output))
}

func handleListSelfCommand(conn net.Conn) {
	if globalNode == nil {
		conn.Write([]byte("Error: Node not available\n"))
		return
	}

	selfID := globalNode.GetID()
	output := fmt.Sprintf("Self ID: %s\n", selfID)
	conn.Write([]byte(output))
}

func handleDisplaySuspectsCommand(conn net.Conn) {
	if globalNode == nil {
		conn.Write([]byte("Error: Node not available\n"))
		return
	}

	members := globalNode.GetMemberList()
	output := "Suspected Nodes:\n"
	output += "================\n"

	suspectedCount := 0
	for _, member := range members {
		if member.State == "Suspected" {
			suspectedCount++
			output += fmt.Sprintf("ID: %s\n", member.ID)
			output += fmt.Sprintf("Address: %s\n", member.Address)
			output += fmt.Sprintf("State: %s\n", member.State)
			output += fmt.Sprintf("Heartbeat: %d\n", member.HeartBeatCounter)
			output += fmt.Sprintf("Incarnation: %d\n", member.Incarnation)
			output += fmt.Sprintf("Last Update: %s\n", member.LastUpdateTime.Format(time.RFC3339))
			output += "---\n"
		}
	}

	if suspectedCount == 0 {
		output += "No suspected nodes found in membership list\n"
	}

	conn.Write([]byte(output))
}

func handleDisplayProtocolCommand(conn net.Conn) {
	if globalNode == nil {
		conn.Write([]byte("Error: Node not available\n"))
		return
	}

	protocol := globalNode.GetProtocol()
	suspicion := globalNode.GetSuspicion()

	output := "Current Protocol Configuration:\n"
	output += "==============================\n"
	output += fmt.Sprintf("Protocol: %s\n", protocol)
	output += fmt.Sprintf("Suspicion: %s\n", suspicion)
	output += "==============================\n"

	conn.Write([]byte(output))
}

func handleSwitchCommand(conn net.Conn, args []string) {
	if globalNode == nil {
		conn.Write([]byte("Error: Node not available\n"))
		return
	}

	if len(args) != 2 {
		conn.Write([]byte("Usage: switch <protocol> <suspicion>\n"))
		conn.Write([]byte("Protocols: gossip, ping\n"))
		conn.Write([]byte("Suspicion: suspect, nosuspect\n"))
		return
	}

	protocol := args[0]
	suspicion := args[1]

	// Remove brackets if present
	protocol = strings.Trim(protocol, "()")
	suspicion = strings.Trim(suspicion, "()")

	// Convert input to match constants
	var protocolConstant, suspicionConstant string

	if protocol == "gossip" {
		protocolConstant = constants.ProtocolGossip
	} else if protocol == "ping" {
		protocolConstant = constants.ProtocolPingAck
	} else {
		conn.Write([]byte("Invalid protocol. Use: gossip or ping\n"))
		return
	}

	if suspicion == "suspect" {
		suspicionConstant = constants.SuspicionTrue
	} else if suspicion == "nosuspect" {
		suspicionConstant = constants.SuspecionFalse
	} else {
		conn.Write([]byte("Invalid suspicion. Use: suspect or nosuspect\n"))
		return
	}

	// Call the Switch method
	globalNode.Switch(protocolConstant, suspicionConstant)
	conn.Write([]byte(fmt.Sprintf("Switched to protocol: %s, suspicion: %s\n", protocol, suspicion)))
}

// Listen creates a TCP listener on the specified address
func Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func main() {
	ln, err := net.Listen("tcp", os.Args[1])
	CheckError(err, "Server cannot listen")
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Accept client failed", err)
			continue
		}
		go Connect2Server(conn)
	}
}

package main

import (
	"flag"
	"fmt"
	"os"

	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/constants"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/distributed_log_querier/server"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/node"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/vmlog"
)

func startDistributedLogQuerierServer(tcpPort string) {
	ln, err := server.Listen("tcp", tcpPort)
	if err != nil {
		fmt.Printf("Cannot start distributed log querier server: %v\n", err)
		return
	}
	fmt.Printf("Distributed log querier server listening on %s\n", tcpPort)
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Accept client failed", err)
			continue
		}
		go server.Connect2Server(conn)
	}
}

func main() {
	port := flag.String("port", "", "listening port")
	introaddr := flag.String("introducer", "", "introducer address")
	tcpPort := flag.String("tcp", "", "TCP port for distributed log querier")
	commandPort := flag.String("commandport", constants.CommandPort, "port for command")
	flag.Parse()

	filename := fmt.Sprintf("machine.%s.log", *port)
	err := vmlog.NewVMLog(filename)
	if err != nil {
		fmt.Println("Cannot create new node")
		os.Exit(1)
	}

	node, err := node.NewNode(*port, *introaddr, *commandPort)
	if err != nil {
		fmt.Println("cannot create new node")
		os.Exit(1)
	}

	//
	server.SetNode(node)
	node.Start()

	// Start the log querier server if TCP port is provided
	if *tcpPort != "" {
		go startDistributedLogQuerierServer(*tcpPort)
	}

	select {}
}

package constants

import (
	"time"
)

const (
	Alive                = "Alive"
	Suspected            = "Suspected"
	Failed               = "Failed"
	ReadTimeOut          = 1 * time.Second
	ListenBufSize        = 4096
	MessagePing          = "ping"
	MessageAck           = "ack"
	MessageGossip        = "gossip"
	MessageJoin          = "join"
	ProtocolPeriod       = 500 * time.Millisecond
	TFail                = 1500 * time.Millisecond
	TSuspect             = 1500 * time.Millisecond
	TCleanup             = 1000 * time.Millisecond
	PingTimeout          = 250 * time.Millisecond
	PingFrequency        = 250 * time.Millisecond
	TSuspectPingAck      = 500 * time.Millisecond
	TFailGossipNosuspect = 3000 * time.Millisecond
	TCleanupPingSuspct   = 500 * time.Millisecond
	GossipNum            = 3
	ProtocolGossip       = "gossip"
	ProtocolPingAck      = "ping"
	SuspicionTrue        = "suspect"
	SuspecionFalse       = "nosuspect"
	DropRate             = 0.0
	ReportInterval       = 10
	Leave                = "Leave"
	CommandPort          = "9080"
	ReplicaNum           = 3
	RPCPort              = "9082"
)

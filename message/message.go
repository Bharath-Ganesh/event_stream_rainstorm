package message

import (
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/member"
)

type Message struct {
	Type       string          `json:"type"`
	Memberlist []member.Member `json:"memberlist"`
	SendID     string          `json:"sendid"`
}

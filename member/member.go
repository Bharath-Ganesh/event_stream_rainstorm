package member

import (
	"time"
)

type Member struct {
	ID               string    `json:"id"`
	Address          string    `json:"addr"`
	HeartBeatCounter int       `json:"heartbeatcounter"`
	Incarnation      int       `json:"incarnation"`
	State            string    `json:"state"`
	LastUpdateTime   time.Time `json:"lastupdatetime"`
}

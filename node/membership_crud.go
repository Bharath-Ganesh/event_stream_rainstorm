package node

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/constants"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/member"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/utils"
	"gitlab.engr.illinois.edu/yonghan4/mp4_g82/vmlog"
)

type MemberList struct {
	mutex           sync.Mutex
	memberlist      map[string]*member.Member
	roundRobinIndex int
	roundRobinList  []*member.Member
	deleteNumber    int
	deleteMutex     sync.Mutex
}

func NewMemberList() *MemberList {
	return &MemberList{
		memberlist:      make(map[string]*member.Member),
		roundRobinIndex: 0,
		roundRobinList:  make([]*member.Member, 0),
		deleteNumber:    0,
	}
}

func (memberlist *MemberList) getSelfByID(nodeID string) (member.Member, bool) {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	nodeSelf, OK := memberlist.memberlist[nodeID]
	if !OK {
		return member.Member{}, false
	}
	return *nodeSelf, true
}

func (memberlist *MemberList) printMemberList() string {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	var output string
	output += "------------------------------\n"
	for _, eachMember := range memberlist.memberlist {
		udpAddr := eachMember.Address
		vmName, check := utils.GetVMName(udpAddr)
		if !check {
			continue
		}
		output += fmt.Sprintf("%s (%s)\n", vmName, eachMember.Address)
	}
	summary := fmt.Sprintf("There are %d members\n", len(memberlist.memberlist))
	output += summary
	output += "End----------------------------"
	return output
}

func (memberlist *MemberList) selectKMember(num int, nodeID string) []*member.Member {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	filterMember := []*member.Member{}
	for _, eachMember := range memberlist.memberlist {
		if nodeID != eachMember.ID && eachMember.State != constants.Failed {
			filterMember = append(filterMember, eachMember)
		}
	}
	lenFilter := len(filterMember)
	rand.Shuffle(lenFilter, func(i, j int) {
		filterMember[i], filterMember[j] = filterMember[j], filterMember[i]
	})
	if lenFilter < num {
		return filterMember
	} else {
		return filterMember[:num]
	}
}

func (memberlist *MemberList) getMemberList() []member.Member {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	outList := []member.Member{}
	for _, member := range memberlist.memberlist {
		outList = append(outList, *member)
	}
	return outList
}

func (memberlist *MemberList) listSuspect() string {
	var output string
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	for _, member := range memberlist.memberlist {
		if member.State == constants.Suspected {
			output += member.ID + "\n"
		}
	}
	if output == "" {
		output = "No suspected nodes now"
	}
	output += "\n"
	return output
}

func (memberlist *MemberList) UpdateMemberlist(senderMemberlist []member.Member, nodeID string) {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	for _, nextMember := range senderMemberlist {
		if nextMember.ID == nodeID {
			currentself, OK := memberlist.memberlist[nodeID]
			if !OK {
				fmt.Println("Cannot find itself in the memberlist")
				return
			}
			if nextMember.State == constants.Suspected && currentself.State == constants.Alive {
				if nextMember.Incarnation >= currentself.Incarnation {
					currentself.Incarnation++
					currentself.LastUpdateTime = time.Now()
					vmlog.RefutedLogger.Printf("NodeID %s still alive\n", nodeID)
				}
			}
			// fmt.Println("Receive node itself")
			continue
		}
		currentMember, exist := memberlist.memberlist[nextMember.ID]
		if exist {
			// need to use hearbeat to check it
			if nextMember.Incarnation > currentMember.Incarnation {
				vmlog.IncarLogger.Printf("MemberID: %s change from %s to %s\n", currentMember.ID, currentMember.State, nextMember.State)
				currentMember.Incarnation = nextMember.Incarnation
				currentMember.State = nextMember.State
				currentMember.HeartBeatCounter = nextMember.HeartBeatCounter
				currentMember.LastUpdateTime = time.Now()
				continue
			}
			if currentMember.State == constants.Failed {
				continue
			}
			if nextMember.State == constants.Failed {
				vmlog.FailedLogger.Printf("%s turned %s to Failed\n", currentMember.ID, currentMember.State)
				currentMember.State = constants.Failed
				currentMember.LastUpdateTime = time.Now()
				continue
			}
			if nextMember.State == constants.Leave {
				vmlog.FailedLogger.Printf("%s turned %s to Failed\n", currentMember.ID, currentMember.State)
				currentMember.State = constants.Failed
				currentMember.LastUpdateTime = time.Now()
				continue
			}
			if nextMember.Incarnation == currentMember.Incarnation {
				if currentMember.State == constants.Alive {
					if nextMember.State == constants.Alive && nextMember.HeartBeatCounter > currentMember.HeartBeatCounter {
						currentMember.HeartBeatCounter = nextMember.HeartBeatCounter
						currentMember.LastUpdateTime = time.Now()
					} else if nextMember.State == constants.Suspected {
						currentMember.State = constants.Suspected
						currentMember.LastUpdateTime = time.Now()
						vmlog.SuspectedLogger.Printf("%s turned Alive to Suspected\n", currentMember.ID)
						fmt.Printf("%s turned Alive to Suspected\n", currentMember.ID)
					}
				} else if currentMember.State == constants.Suspected {
					// suspected to suspected/alive did nothing
				}
			}
		} else {
			if nextMember.State != constants.Failed {
				nextMember.LastUpdateTime = time.Now()
				newAddMember := nextMember
				memberlist.memberlist[nextMember.ID] = &newAddMember
				vmlog.JoinLogger.Printf("Node ID %s add new ID %s\n", nodeID, nextMember.ID)
				//fmt.Printf("[Add] node ID %s add new ID %s\n", nodeID, nextMember.ID)
			}
		}
	}
}

func (memberlist *MemberList) changeState(targetMemberID string, prevState string, newState string) []member.Member {
	memberlist.mutex.Lock()
	//defer node.mutex.Unlock()
	changeMember, OK := memberlist.memberlist[targetMemberID]
	if !OK || changeMember.State != prevState {
		memberlist.mutex.Unlock()
		return []member.Member{}
	}
	changeMember.State = newState
	changeMember.LastUpdateTime = time.Now()
	if newState == constants.Failed {
		vmlog.FailedLogger.Printf("MemberID: %s from prevState %s to newState %s\n", changeMember.ID, prevState, newState)
	} else if newState == constants.Suspected {
		vmlog.SuspectedLogger.Printf("MemberID: %s from prevState %s to newState %s\n", changeMember.ID, prevState, newState)
		fmt.Printf("MemberID: %s from prevState %s to newState %s\n", changeMember.ID, prevState, newState)
	}
	updateMember := *changeMember
	memberlist.mutex.Unlock()
	return []member.Member{updateMember}
}

func (memberlist *MemberList) TimeOutCheck(protocol string, suspicion string) []member.Member {
	memberlist.mutex.Lock()
	failedMemberList := []member.Member{}
	deleteMemberID := []string{}
	for _, member := range memberlist.memberlist {
		if member.State == constants.Failed {
			if protocol == constants.ProtocolPingAck && suspicion == constants.SuspicionTrue {
				if time.Since(member.LastUpdateTime) > constants.TCleanupPingSuspct {
					vmlog.DeletedLogger.Printf("[Timeout] ID %s are deleted\n", member.ID)
					fmt.Printf("[Timeout] ID %s are deleted\n", member.ID)
					deleteMemberID = append(deleteMemberID, member.ID)
					memberlist.deleteMutex.Lock()
					memberlist.deleteNumber++
					memberlist.deleteMutex.Unlock()
				}
			} else if time.Since(member.LastUpdateTime) > constants.TCleanup {
				vmlog.DeletedLogger.Printf("[Timeout] ID %s are deleted\n", member.ID)
				fmt.Printf("[Timeout] ID %s are deleted\n", member.ID)
				deleteMemberID = append(deleteMemberID, member.ID)
				memberlist.deleteMutex.Lock()
				memberlist.deleteNumber++
				memberlist.deleteMutex.Unlock()
			}
		}
		if member.State == constants.Alive && protocol == constants.ProtocolGossip {
			if suspicion == constants.SuspicionTrue && time.Since(member.LastUpdateTime) > constants.TFail {
				member.State = constants.Suspected
				member.LastUpdateTime = time.Now()
				vmlog.SuspectedLogger.Printf("[Gossip Timeout] ID %s turn Alive to Suspected\n", member.ID)
				fmt.Printf("[Gossip Timeout] ID %s turn Alive to Suspected\n", member.ID)
			} else if suspicion == constants.SuspecionFalse && time.Since(member.LastUpdateTime) > constants.TFailGossipNosuspect {
				member.State = constants.Failed
				member.LastUpdateTime = time.Now()
				vmlog.FailedLogger.Printf("[Gossip Timeout] ID %s turn Alive to Failed\n", member.ID)
				failedMemberList = append(failedMemberList, *member)
			}
		}
		if member.State == constants.Suspected && suspicion == constants.SuspicionTrue {
			if protocol == constants.ProtocolGossip && time.Since(member.LastUpdateTime) > constants.TSuspect {
				vmlog.FailedLogger.Printf("[%s Failed Timeout] ID %s turn Suspected to Failed\n", protocol, member.ID)
				member.State = constants.Failed
				member.LastUpdateTime = time.Now()
				failedMemberList = append(failedMemberList, *member)
			} else if protocol == constants.ProtocolPingAck && time.Since(member.LastUpdateTime) > constants.TSuspectPingAck {
				vmlog.FailedLogger.Printf("[%s Failed Timeout] ID %s turn Suspected to Failed\n", protocol, member.ID)
				member.State = constants.Failed
				member.LastUpdateTime = time.Now()
				failedMemberList = append(failedMemberList, *member)
			}
		}
	}
	for _, deleteID := range deleteMemberID {
		delete(memberlist.memberlist, deleteID)
	}
	memberlist.mutex.Unlock()
	return failedMemberList
}

func (memberlist *MemberList) addHeartBeat(nodeID string) {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	nodeitself, OK := memberlist.memberlist[nodeID]
	if OK {
		nodeitself.HeartBeatCounter++
		nodeitself.LastUpdateTime = time.Now()
	}
}

func (memberlist *MemberList) updateSenderTime(nodeID string, sendID string) {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	nodeMember, OK := memberlist.memberlist[sendID]
	if OK {
		nodeMember.LastUpdateTime = time.Now()
		vmlog.ReceivedLogger.Printf("%s receive message from %s", nodeID, sendID)
	}
}

func (memberlist *MemberList) roundRobinK(num int, nodeID string) []*member.Member {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	if memberlist.roundRobinIndex >= len(memberlist.roundRobinList) {
		memberlist.permutation(nodeID)
	}

	if len(memberlist.roundRobinList) == 0 {
		return nil
	}
	lenroundRobinList := len(memberlist.roundRobinList)
	if num >= lenroundRobinList {
		memberlist.roundRobinIndex = lenroundRobinList
		return memberlist.roundRobinList
	}
	selectedMember := make([]*member.Member, 0)
	for i := 0; i < num; i++ {
		if memberlist.roundRobinIndex >= lenroundRobinList {
			memberlist.permutation(nodeID)
			if len(memberlist.roundRobinList) == 0 {
				break
			}
			lenroundRobinList = len(memberlist.roundRobinList)
		}
		selectedMember = append(selectedMember, memberlist.roundRobinList[memberlist.roundRobinIndex])
		memberlist.roundRobinIndex++
	}
	return selectedMember
}

func (memberlist *MemberList) permutation(nodeID string) {
	var newRoundRobinList []*member.Member
	for _, eachMember := range memberlist.memberlist {
		if eachMember.ID != nodeID && eachMember.State != constants.Failed {
			newRoundRobinList = append(newRoundRobinList, eachMember)
		}
	}
	lenNewList := len(newRoundRobinList)
	if lenNewList > 0 {
		rand.Shuffle(lenNewList, func(i, j int) {
			newRoundRobinList[i], newRoundRobinList[j] = newRoundRobinList[j], newRoundRobinList[i]
		})
	}
	memberlist.roundRobinList = newRoundRobinList
	memberlist.roundRobinIndex = 0
	//fmt.Printf("Re shuffle the membership list")
}

func (memberlist *MemberList) resetOtherTime(nodeID string) {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	currentTime := time.Now()
	for machineID, eachMember := range memberlist.memberlist {
		if machineID != nodeID {
			eachMember.LastUpdateTime = currentTime
		}
	}
}

func (memberlist *MemberList) TurnLeave(nodeID string) {
	memberlist.mutex.Lock()
	defer memberlist.mutex.Unlock()
	nodeitself, OK := memberlist.memberlist[nodeID]
	if OK {
		nodeitself.State = constants.Leave
		nodeitself.LastUpdateTime = time.Now()
	}
}

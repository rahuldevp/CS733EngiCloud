package raft

import(
	"strconv"
	"log"
	"net/rpc"
	"sync"
	"fmt"
	"time"
	"sort"
	"math/rand"
)

type Lsn int //Log sequence number, unique for all time.

var unique_lsn Lsn = 1000

var voteCount = 1

var timer *time.Timer

var HBtimer = time.NewTimer(HeartBeatInterval)

type ErrRedirect int // See Log.Append. Implements Error interface.

//defining the mutex to be used for RW operation for multiple clients
var mutex = &sync.RWMutex{}

const ( 
	MajorityCount = 3 
	Follower = "Follower"
	Candidate = "Candidate"
	Leader = "Leader"
	HeartBeatInterval = 100 * time.Millisecond
)

//Structure for VotingRPC
type VotingStruct struct {
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//Structure for VotingRPCReply
type VotingStructReply struct {
	Term int
	IsVoted bool
}

//Structure for AppendLogEntry
type AppendLogEntry struct {
	Term int
	LeaderID int
	PreviousLogIndex int
	PreviousLogTerm int
	LastCommitIndex int
	LogEntries []LogEntity
}

//Structure for reply of AppendLogEntry
type AppendLogEntryReply struct {
	Term int
	IsCommited bool
	IsHB bool
}

type LogEntry interface {
	Lsn() Lsn
	Data() []byte
	Committed() bool
}

type SharedLog interface {
// Each data item is wrapped in a LogEntry with a unique
// lsn. The only error that will be returned is ErrRedirect,
// to indicate the server id of the leader. Append initiates
// a local disk write and a broadcast to the other replicas,
// and returns without waiting for the result.
	Append(data []byte) (LogEntry, error)
}

// --------------------------------------

//structure for log entry
type LogEntity struct {
	LogIndex int
	Term int
	Data []byte
	Committed bool
}

// Raft setup
type ServerConfig struct {
	Id int // Id of server. Must be unique
	Host string // name or ip of host
	ClientPort int // port at which server listens to client messages.
	LogPort int // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path string // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

// Raft implements the SharedLog interface.
type Raft struct {
	// .... fill
	Clusterconfig ClusterConfig
	CommitCh chan LogEntity
	ServerId int
	State string
	ElectionTimeoutInterval time.Duration

	//array of log entries maintained by each server
	Log []LogEntity
	VotingCh chan VotingStruct
	ReplyVotingCh chan VotingStructReply
	AppendLogCh chan AppendLogEntry
	ReplyAppendLogCh chan AppendLogEntryReply
	
	CurrentTerm int
	VotedForServer int
	Leader int

	CommitIndex int
	LastApplied int

	NextIndex []int
	MatchIndex []int

	ClientToLeader chan []byte
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int) (*Raft, error) {
	
	var raft Raft
	raft.Clusterconfig = *config
	raft.CommitCh = make(chan LogEntity,1000)
	raft.ServerId = thisServerId
	raft.State = Follower
	raft.ElectionTimeoutInterval = (300 * time.Millisecond) + (time.Duration(thisServerId) * 300 * time.Millisecond)
	raft.VotingCh = make(chan VotingStruct,1000)
	raft.ReplyVotingCh = make(chan VotingStructReply,1000)
	raft.AppendLogCh = make(chan AppendLogEntry,1000)
	raft.ReplyAppendLogCh = make(chan AppendLogEntryReply,1000)
	raft.VotedForServer = -1
	raft.CurrentTerm = 0
	raft.CommitIndex = 0
	raft.LastApplied = 0
	raft.Leader = -1
	
	for k:=1; k<=100; k++ {
		raft.Log = append(raft.Log, LogEntity{k,-9,[]byte{'a','b','c'},true})
	}

	raft.NextIndex = make([]int, 5)
	raft.MatchIndex = make([]int, 5)

	raft.ClientToLeader = make(chan []byte,1024)
	return &raft, nil
}

func Loop(raft_obj *Raft) {
	timer = time.NewTimer(raft_obj.ElectionTimeoutInterval)
    var state string
    state = Follower
    for {
        switch (state)  {
        case Follower: FollowerLoop(raft_obj)
        case Candidate: CandidateLoop(raft_obj)
        case Leader: LeaderLoop(raft_obj)
        default: return
        }
        state = raft_obj.State
    }
}

var ackCount =make(chan int,10)

//var ack =make(chan int,10)

func FollowerLoop(raft *Raft) {
	timer.Reset(raft.ElectionTimeoutInterval)
	//fmt.Println("I am a follower", raft.ServerId, "--", raft.CurrentTerm)
	var request string
    var votingEvent VotingStruct
    var appendLogEvent AppendLogEntry
    ThisLoop:
    for raft.State == Follower  {
    	randNum := rand.Intn(1000)
		if randNum > 900 {
			time.Sleep(500*time.Millisecond)
		}
        select
        {
        case votingEvent = <-raft.VotingCh:
        	request = "VoteRequest"
        	//timer.Reset(raft.ElectionTimeoutInterval)
        case appendLogEvent = <-raft.AppendLogCh:
        	request = "AppendRequest"
        	//timer.Reset(raft.ElectionTimeoutInterval)
        case <-time.After(10*time.Millisecond):
        case <-timer.C:
        	raft.State = Candidate
        	break ThisLoop
        }
        switch request {
        case "AppendRequest":
        	receiverLastLogIndex := len(raft.Log)
            //receiverLastLogTerm := raft.Log[receiverLastLogIndex-1].Term
        	if len(appendLogEvent.LogEntries)==0 {
        		if appendLogEvent.Term < raft.CurrentTerm {
        		raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,true}
        		} else if appendLogEvent.Term >= raft.CurrentTerm {
        			raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,true,true}
		    		raft.CurrentTerm = appendLogEvent.Term
		    		raft.Leader = appendLogEvent.LeaderID
		    		break ThisLoop
        		}
    		} else {
				if appendLogEvent.Term < raft.CurrentTerm {
			        raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,false}
			    } else if receiverLastLogIndex < appendLogEvent.PreviousLogIndex {
			        raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,false}
			    } else if receiverLastLogIndex > appendLogEvent.PreviousLogIndex {
			        if raft.Log[appendLogEvent.PreviousLogIndex].Term != appendLogEvent.PreviousLogTerm {
			           raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,false}
			        } else {
			        	//fmt.Println("*******************************************************")
			        	fmt.Println(raft.ServerId,"-----",appendLogEvent.LogEntries[0].LogIndex,"------",appendLogEvent.LogEntries[0].Data)
			            raft.Log[appendLogEvent.PreviousLogIndex]=appendLogEvent.LogEntries[0]
			            raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,true,false}
			        }
			    } else if receiverLastLogIndex == appendLogEvent.PreviousLogIndex {
			        if receiverLastLogIndex!=0 && raft.Log[receiverLastLogIndex].Term !=  appendLogEvent.PreviousLogTerm{
			           raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,false}
			        } else {
			        	//fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
			        	fmt.Println(raft.ServerId,"-----",appendLogEvent.LogEntries[0].LogIndex,"------",appendLogEvent.LogEntries[0].Data)
			            raft.Log=append(raft.Log,appendLogEvent.LogEntries[0])
			            raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,true,false}
			        }
			    }
    		}
    		timer.Reset(raft.ElectionTimeoutInterval)
       	case "VoteRequest":
            receiverLastLogIndex := len(raft.Log)
            receiverLastLogTerm := raft.Log[receiverLastLogIndex-1].Term
       		if votingEvent.Term > raft.CurrentTerm {
            	raft.CurrentTerm = votingEvent.Term
            }
       		if votingEvent.Term < raft.CurrentTerm {
            	raft.ReplyVotingCh<-VotingStructReply{raft.CurrentTerm,false}
            } else if raft.VotedForServer != -1 && raft.VotedForServer != votingEvent.CandidateID {
            	raft.ReplyVotingCh<-VotingStructReply{raft.CurrentTerm,false}
            } else if receiverLastLogIndex > votingEvent.LastLogIndex || receiverLastLogTerm > votingEvent.LastLogTerm {
            	raft.ReplyVotingCh<-VotingStructReply{raft.CurrentTerm,false}
            } else {
            	raft.VotedForServer = votingEvent.CandidateID
            	raft.ReplyVotingCh<-VotingStructReply{raft.CurrentTerm,true}
            }
    		timer.Reset(raft.ElectionTimeoutInterval)
        default: 
        	
    	}
    	request = ""
    }
}

func CandidateLoop(raft *Raft) {
	timer.Reset(raft.ElectionTimeoutInterval)
	raft.CurrentTerm++
	//fmt.Println("I am a Candidate", raft.ServerId, "--", raft.CurrentTerm)
	votingProcess := true
	raft.VotedForServer = -1
	//if (votingProcess == false) {}
	voteCount = 1
	var request string
    var votingEvent VotingStruct
    var appendLogEvent AppendLogEntry
    ThisLoop:
	for raft.State == Candidate {
		select
        {
        case votingEvent = <-raft.VotingCh:
        	request = "VoteRequest"
        	//timer.Reset(raft.ElectionTimeoutInterval)
        case appendLogEvent = <-raft.AppendLogCh:
        	request = "AppendRequest"
        case <-time.After(10*time.Millisecond):
        case <-timer.C:
        	raft.State = Candidate
        	break ThisLoop
        }
        switch request {
        case "AppendRequest":
        	if len(appendLogEvent.LogEntries)==0 {
        		if appendLogEvent.Term < raft.CurrentTerm {
	        		raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,true}
	        	} else if appendLogEvent.Term >= raft.CurrentTerm {
	        		raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,true}
	        		raft.CurrentTerm = appendLogEvent.Term
	        		raft.Leader = appendLogEvent.LeaderID
	        		raft.VotedForServer = appendLogEvent.LeaderID
	        		raft.State = Follower
	        		break ThisLoop
        		}
        	} else {
        		if appendLogEvent.Term < raft.CurrentTerm {
	        		raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,false}
	        	} else if appendLogEvent.Term >= raft.CurrentTerm {
	        		raft.ReplyAppendLogCh<-AppendLogEntryReply {raft.CurrentTerm,false,false}
	        		raft.CurrentTerm = appendLogEvent.Term
	        		raft.Leader = appendLogEvent.LeaderID
	        		raft.VotedForServer = appendLogEvent.LeaderID
	        		raft.State = Follower
	        		break ThisLoop
        		}
        	}
        	
       	case "VoteRequest":
       		receiverLastLogIndex := len(raft.Log)
            receiverLastLogTerm := raft.Log[receiverLastLogIndex-1].Term
       		if votingEvent.Term > raft.CurrentTerm {
            	raft.CurrentTerm = votingEvent.Term
            }
       		if votingEvent.Term < raft.CurrentTerm {
            	raft.ReplyVotingCh<-VotingStructReply{raft.CurrentTerm,false}
            } else if raft.VotedForServer != -1 && raft.VotedForServer != votingEvent.CandidateID {
            	raft.ReplyVotingCh<-VotingStructReply{raft.CurrentTerm,false}
            } else if receiverLastLogIndex > votingEvent.LastLogIndex || receiverLastLogTerm > votingEvent.LastLogTerm {
            	raft.ReplyVotingCh<-VotingStructReply{raft.CurrentTerm,false}
            } else {
            	raft.VotedForServer = votingEvent.CandidateID
            	raft.ReplyVotingCh<-VotingStructReply{raft.CurrentTerm,true}
            	raft.State = Follower
    			break ThisLoop
            }

        default: 
        	if votingProcess == true {
				raft.VotedForServer = raft.ServerId
				votingStruct := VotingStruct{
					raft.CurrentTerm,
					raft.ServerId,
					100,
					101,
				}

				cc := raft.Clusterconfig
				for _,value := range cc.Servers {
					if(value.Id != raft.ServerId) {
						go sendVoteRPC(votingStruct, value, raft)
					}
					//fmt.Println("In voting process: ServerId--RPCToWhichServer-----",raft.ServerId, value.Id)	
				}
			}
			votingProcess = false
       	}
       	if voteCount >= MajorityCount {
			fmt.Println("Got majority")
			raft.State = Leader
			return
		}
	}
}

var data []byte 
func LeaderLoop(raft *Raft) {
	HBtimer.Reset(HeartBeatInterval)
	fmt.Println("I am a Leader-----", raft.ServerId, "--", raft.CurrentTerm)
	/*for kk:=0; kk<5; kk++ {
		raft.NextIndex[kk] = raft.CommitIndex
	}*/
	var request string
    var votingEvent VotingStruct
    var appendLogEvent AppendLogEntry
    var randNum int
    ThisLoop:
	for raft.State == Leader {
		randNum = rand.Intn(100)
		if randNum > 97 {
			raft.State = Follower
			break ThisLoop
		}
		select
        {
 		case votingEvent = <-raft.VotingCh:
        	request = "VoteRequest"
        	//timer.Reset(raft.ElectionTimeoutInterval)
        case appendLogEvent = <-raft.AppendLogCh:
        	request = "AppendRequest"
        case <-time.After(10*time.Millisecond):
        }
        switch request {
        case "VoteRequest":
        	if votingEvent.Term <= raft.CurrentTerm {
            	//respond with 
            	temp := VotingStructReply{
      			raft.CurrentTerm,
      			false,
    			}
    			raft.ReplyVotingCh<-temp
            } else if votingEvent.Term > raft.CurrentTerm {
            	//upgrade currentterm
            	raft.CurrentTerm = votingEvent.Term
            	//OldTime = time.Now()
            	temp := VotingStructReply{
      			raft.CurrentTerm,
      			true,
    			}
    			raft.ReplyVotingCh<-temp
    			raft.State = Follower
    			//fmt.Println("************************************************************************")
    			break ThisLoop
                //reply ok to event.msg.serverid
                //remember term, leader id (either in log or in separate file)
            }
        case "AppendRequest":
        	if appendLogEvent.Term <= raft.CurrentTerm {

        	} else if appendLogEvent.Term > raft.CurrentTerm {
        		raft.State = Follower
        		//fmt.Println("------------------------------------------------------------------------")
        		break ThisLoop
        	}
        }
    	select 
    	{
    		case <-HBtimer.C:
    			//fmt.Println("--------------------------------------------------------------",raft.ServerId)
				var tempLog []LogEntity
				cc := raft.Clusterconfig
				for _,value := range cc.Servers {
					appendStruct := AppendLogEntry{
					raft.CurrentTerm,
					raft.ServerId,
					len(raft.Log)-1,
					raft.Log[len(raft.Log)-1].Term,
					raft.CommitIndex,
					append(tempLog,raft.Log[raft.NextIndex[value.Id]]),
					}
					if(value.Id != raft.ServerId) {
						go sendAppendRPC(appendStruct, value, raft)
					}
					tempLog = nil
				}
				HBtimer.Reset(HeartBeatInterval)
    		case <-time.After(5*time.Millisecond):
    	}
    	
    	select
    	{
    		case data=<-raft.ClientToLeader:
    			tempLogEntity := LogEntity{len(raft.Log), raft.CurrentTerm, data, false}
    			raft.Log = append(raft.Log, tempLogEntity)
    		case <-time.After(5*time.Millisecond):
    	}
    	setCommitIndex(raft,data)
	}
}

func setCommitIndex(raft *Raft, data []byte) {
	vals := raft.MatchIndex
	sort.Ints(vals)
	if vals[3] > raft.CommitIndex && raft.Log[vals[3]].Term == raft.CurrentTerm {
		raft.CommitIndex = vals[3]
		raft.CommitCh<-LogEntity{raft.CommitIndex, raft.CurrentTerm, data, true}
	}
} 

func sendAppendRPC(appendLogEntry AppendLogEntry, value ServerConfig, raft *Raft) {
//not to send the append entries rpc to the leader itself 
	
	//fmt.Println("Sending RPCs to:",value.Host+":"+strconv.Itoa(value.LogPort))	
	
	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	defer client.Close() 
	if err != nil {
		log.Print("Error Dialing :", err)
	}
	 // Synchronous call
	args := &appendLogEntry
	
	//this reply is the ack from the followers receiving the append entries
	var reply AppendLogEntryReply

	err = client.Call("Temp.AppendLogRPC", args, &reply) 

	if err != nil {
		log.Print("Remote Method Invocation Error:", err)
	} else {
		if reply.Term > raft.CurrentTerm {
			raft.State = Follower
		} else if reply.IsCommited == false && reply.IsHB == false {
			if raft.NextIndex[value.Id] >= 0 {
				mutex.Lock()
				raft.NextIndex[value.Id]--
				mutex.Unlock()
			}
		} else if reply.IsCommited == true {
			if len(raft.Log) > raft.NextIndex[value.Id]{
				//fmt.Println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
				mutex.Lock()
				raft.NextIndex[value.Id]++
				mutex.Unlock()
				raft.MatchIndex[value.Id]=raft.NextIndex[value.Id]	
			}
		}
	}
}

func sendVoteRPC(votingStruct VotingStruct, value ServerConfig, raft *Raft) {
	client, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	defer client.Close()
	if err != nil {
		log.Print("Error Dialing :", err)
	}
	 // Synchronous call
	args := &votingStruct
	
	//this reply is the ack from the followers receiving the append entries
	var reply VotingStructReply

	err = client.Call("Temp.VotingRPC", args, &reply) 

	if err != nil {
		log.Print("Remote Method Invocation Error:", err)
	}
	//fmt.Println("Received reply for(sendVoteRPC method): CandidateID--ServerID--Reply(IsVoted)--Reply(Term)-----",raft.ServerId,value.Id, reply.IsVoted, reply.Term)
	if(reply.IsVoted) {
		// Means voted yes
		mutex.Lock()
		voteCount++
		mutex.Unlock()
	} else {
	}
}

//raft implementing the shared log interface
func (raft *Raft) Append(data []byte) (LogEntity,error) {

	//fmt.Println("Ready to append the data")
	
	logEntity:= LogEntity{
					-5,
					-5,
					data,
					false,
					}
	//locking the access to the log of leader for multiple clients
	/*mutex.Lock()
	raft.Log=append(raft.Log,logEntity)

	cc := raft.Clusterconfig

	for _,value := range cc.Servers {
		if(value.Id != raft.ServerId) {
			go sendRpc(value,logEntity)
			<- ackCount
		}
	}

	//incrementing the log sequence for every append request
	unique_lsn++

	//the majority acks have been received
	raft.CommitCh <- logEntity

	//fmt.Println("Released the lock")

	mutex.Unlock()*/
	return logEntity,nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}
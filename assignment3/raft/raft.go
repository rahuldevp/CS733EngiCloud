package raft

import(
	"strconv"
	"log"
	"net/rpc"
	"sync"
	"time"
	"fmt"
)

const (
	HeartBeatInterval = 100 * time.Millisecond

	MajorityCount = 3
)

type Lsn uint64 //Log sequence number, unique for all time.

var ElectionTimeoutInterval time.Duration

var unique_lsn Lsn = 1000

type ErrRedirect int // See Log.Append. Implements Error interface.

//defining the mutex to be used for RW operation for multiple clients
 var mutex = &sync.RWMutex{}

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

//Structure for VotingRPC
type VotingStruct struct {
	Term uint64
	CandidateID int
	PreviousLogIndex uint64
	PreviousLogTerm uint64
}

type VotingStructReply struct {
	Term uint64
	IsVoted bool
}

//structure for log entry
type LogEntity struct {
	Loglsn Lsn
	Data []byte
	Committed bool
}

type EventStructLog struct {
	Term uint64
	Message string
	LogForEvents LogEntity
}

type EventStruct struct {
	Term uint64
	CandidateID int
	Message string
}

type ReplyEventStruct struct {
	Term uint64
	IsVoted bool
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
	//array of log entries maintained by each server
	Log []LogEntity
	ElectionTimeout time.Duration
	HeartBeat time.Duration
	State string
	EventCh chan EventStruct
	ReplyEventCh chan ReplyEventStruct
	CurrentTerm uint64
	VotedForServer int
	Leader string
}

var ackCount =make(chan int)

//raft implementing the shared log interface
func (raft *Raft) Append(data []byte) (LogEntity,error) {

	//fmt.Println("Ready to append the data")
	
	logEntity:= LogEntity{
					unique_lsn,
					data,
					false,
					}
	//locking the access to the log of leader for multiple clients
	mutex.Lock()
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

	mutex.Unlock()
	return logEntity,nil
}


// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntity) (*Raft, error) {
	fmt.Println("NewRaft object created for server",thisServerId)
	var raft Raft
	raft.Clusterconfig=*config
	raft.CommitCh=commitCh
	raft.ServerId=thisServerId
	raft.State="Follower"
	raft.ElectionTimeout=ElectionTimeoutInterval
	raft.HeartBeat=HeartBeatInterval
	raft.EventCh = make(chan EventStruct,1000)
	raft.ReplyEventCh = make(chan ReplyEventStruct,1000)
	raft.VotedForServer = -1
	raft.CurrentTerm = 0
	return &raft, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(10)
}

func Loop(raft_obj *Raft) {
	ElectionTimeoutInterval = (300 * time.Millisecond) + (time.Duration(raft_obj.ServerId) * 100 * time.Millisecond)
    raft_obj.ElectionTimeout = ElectionTimeoutInterval
    raft_obj.State = "Follower" // begin life as a follower
    var state string
    state = "Follower"
    for {
        switch (state)  {
        case "Follower": FollowerLoop(raft_obj)
        case "Candidate": CandidateLoop(raft_obj)
        case "Leader": LeaderLoop(raft_obj)
        default: return
        }
        state = raft_obj.State
    }
}

func FollowerLoop(raft *Raft) {
	fmt.Println("I am a follower", raft.ServerId)
    //var OldTime = time.Now()
    timer := time.NewTimer(ElectionTimeoutInterval)
    var event EventStruct
    for raft.State == "Follower"  {
        //fmt.Println("here----", raft.ServerId)
        select
        {
        case event = <-raft.EventCh:
        	//fmt.Println("-----------:",event.Message)
        case <-time.After(10*time.Millisecond):
        case <- timer.C:
        	raft.State = "Candidate"
        	break
        }
        switch event.Message {
        case "ClientAppend":
        	fmt.Println("+++++++++++++++")
        	//ClientAppendMethod(raft)
            // Do not handle clients in follower mode. Send it back up the
            // pipe with committed = false
            //event.LogForEvents.Committed = false
            //raft.CommitCh <- event.LogForEvents

        case "AppendRPC":
        	fmt.Println("****************")
        	AppendRPCMethod(raft)
            //reset timer
            //if msg.term < currentterm, ignore
            //reset heartbeat timer
            //upgrade to event.msg.term if necessary
            //if prev entries of my log and event.msg match
               //add to disk log
               //flush disk log
               //respond ok to event.msg.serverid
            //else
               //respond err.
       	case "VoteRequest":
       		//fmt.Println("Here------------------")
            if event.Term < raft.CurrentTerm {
            	//respond with 
            	temp := ReplyEventStruct{
      			raft.CurrentTerm,
      			false,
    			}
    			raft.ReplyEventCh<-temp
            } else if event.Term >= raft.CurrentTerm {
            	//upgrade currentterm
            	raft.CurrentTerm = event.Term
            	//OldTime = time.Now()
            	temp := ReplyEventStruct{
      			raft.CurrentTerm,
      			true,
    			}
    			raft.ReplyEventCh<-temp
                //reply ok to event.msg.serverid
                //remember term, leader id (either in log or in separate file)
            }
            event.Message = ""
        /*default: 
        	var NewTime = time.Now()
    		if NewTime.Sub(OldTime) > ElectionTimeoutInterval {
    			raft.State = "Candidate"
    			break
    		}*/
    	}
    	//Check for heartbeat
    }
}

func LeaderLoop(raft *Raft) {
	fmt.Println("I am a Leader-----", raft.ServerId)
	for raft.State == "Leader"  {

	}
}

var ack = make(chan int)

func CandidateLoop(raft *Raft) {
	fmt.Println("I am a Candidate", raft.ServerId)
	timer1 := time.NewTimer(ElectionTimeoutInterval)
	//var OldTime = time.Now()
	var votingProcess = true
	var voteCount = 1
	for raft.State == "Candidate"  {
		var dummy = make(chan int,1)
		if votingProcess==true {
			dummy<-1
		}
		select {
			case <-dummy:
				fmt.Println("Here--------------")
				raft.CurrentTerm++	
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
						go sendVoteRPC(votingStruct, value, raft, ack)
						voteCount = voteCount + <- ack
					}
					fmt.Println("In voting process: ServerId--RPCToWhichServer-----",raft.ServerId, value.Id)	
					if voteCount >= MajorityCount {
						fmt.Println("Got majority")
						raft.State = "Leader"
						return
					}
				}
				fmt.Println("Out of voting process: ServerId----",raft.ServerId)
				//for --- ask for votes
				//voteCount = voteCount + n
				votingProcess = false
			case <-timer1.C:
				votingProcess = true
        	case <-time.After(10*time.Millisecond):
		}
		
		//default: 
        	/*var NewTime = time.Now()
    		if NewTime.Sub(OldTime) > ElectionTimeoutInterval {
    			//raft.CurrentTerm++
    			votingProcess = true
    		}*/

		/*case "VoteRequest":
        	VoteRequestMethod(raft)
          	//msg = event.msg
            //if msg.term < currentterm, respond with 
            //if msg.term > currentterm, upgrade currentterm
            //if not already voted in my term
                //reset timer
                //reply ok to event.msg.serverid
                //remember term, leader id (either in log or in separate file)*/
		//If timeout then 
		//votingProcess = true
	}
}

func sendVoteRPC(votingStruct VotingStruct, value ServerConfig, raft *Raft, ack chan int) {
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
	fmt.Println("Received reply for(sendVoteRPC method): ServerID--Reply(IsVoted)--Reply(Term)-----",value.Id, reply.IsVoted, reply.Term)
	if(reply.IsVoted) {
		// Means voted yes
		ack <- 1
	} else {
		ack <- 0
	}
}

func sendRpc(value ServerConfig,logEntity LogEntity) {
//not to send the append entries rpc to the leader itself 
	
	//fmt.Println("Sending RPCs to:",value.Host+":"+strconv.Itoa(value.LogPort))	
	
	client1, err := rpc.Dial("tcp", value.Host+":"+strconv.Itoa(value.LogPort))
	defer client1.Close()
	if err != nil {
		log.Print("Error Dialing :", err)
	}
	 // Synchronous call
	args := &logEntity
	
	//this reply is the ack from the followers receiving the append entries
	var reply bool

	err = client1.Call("Temp.AcceptLogEntry", args, &reply) 

	if err != nil {
		log.Print("Remote Method Invocation Error:", err)
	}
	replyCount:=0

	if(reply) {
		//fmt.Println("Received reply for:",value.Id)
		replyCount++
		ackCount <- replyCount
	}
}

/*func ClientAppendMethod(raft *Raft) {

}*/

func VoteRequestMethod(raft *Raft) {
	// Use RPC to get votes from other servers available
}

func AppendRPCMethod(raft *Raft) {

}
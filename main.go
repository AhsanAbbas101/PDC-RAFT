package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

//
type Peer struct {
	//conn net.Conn // Connection to handle heartbeats effectively  ... // How Maintain Persistant ?
	Port string // identity
}

//
type Message struct {
	MessageType MessageTypeEnum
	Data        interface{}
}

type Vote struct {
	Sender string
	// port ?
	Term int
}

type Command struct {
	Operation OperationEnum
	Data      int
}

type ChangeLog struct {
	CommandExec  Command
	Index        int
	Commit       bool
	Contributors int
}

type UpdateLogStruct struct {
	Log         map[int]ChangeLog
	LastIndex   int
	CurrentData int
}

var (
	port          string
	originalPeers []Peer
	peers         []Peer
	blackoutPeers []Peer
	leader        *Peer = nil
	stopOnce      bool  = false

	MajorityCount int

	interval int = 5
	offset   int = 8

	state     StateEnum = Follower
	term      int       = 0
	voteCount int       = 1

	addVote                      chan Vote
	voteRequest_chan             chan Vote
	appointLeader                chan string
	stopHeartBeat                chan bool
	appendEntry_chan             chan Vote
	appendEntryData_chan         chan ChangeLog
	appendEntryResponseData_chan chan ChangeLog

	blackout_chan        chan Peer
	goDark_chan          chan Peer
	letThereBeLight_chan chan bool
)
var (
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
)

var (
	data            int               // Replicate Data on a node.
	entryQueue_chan chan ChangeLog    // Unapproved Log Entries
	entryLog        map[int]ChangeLog // Approved Log Entries
	lastLogIndex    int
)

//-----------------------------------------------------------------------------

/*
	Initializes Peer Objects
*/
func initPeers(dailPorts []string) {

	//time.Sleep(3 * time.Second)

	for _, port := range dailPorts {
		/*
			conn, err := net.Dial("tcp", "localhost:"+port)
			if err != nil {
				// handle error
				fmt.Println("Error in Dial")
				continue
			}
		*/
		//var conn net.Conn
		originalPeers = append(originalPeers, Peer{port})
		//peers = append(peers, Peer{port})
	}
	peers = originalPeers
	MajorityCount = (len(peers)+1)/2 + 1

}

/*
	Sends Message on Peer port
	args := Message - message to send
*/
func (p Peer) sendMessage(msg Message) {

	conn, err := net.Dial("tcp", "localhost:"+p.Port)
	if err != nil {
		fmt.Println("[E] Error Dailing Port ", p.Port)
		return
	}

	gobEncoder := gob.NewEncoder(conn)
	err = gobEncoder.Encode(msg)
	if err != nil {
		log.Println(err)
	}

	defer conn.Close()
}

/*
	Recieves Message from Connection
	args := conn - Connection to recieve from
	returns := Message , Bool (if successfully read)
*/
func recvMessage(conn net.Conn) (Message, bool) {

	var msg Message

	dec := gob.NewDecoder(conn)
	err := dec.Decode(&msg)

	if err != nil {
		//handle error
		fmt.Println("Error IN Decode_ Recv Message")
		fmt.Println(err)
		return msg, false
	}
	return msg, true
}

// ------------------------------------------------------------------------------------------

/*
	Starts Election Term
	Sends Request Vote to Every Peer
*/
func startElection() {

	state = Candidate

	fmt.Println("[1] Starting election with term ", term)

	fmt.Println("[->] Sending RequestVote to Peers ", term)
	for _, peer := range peers {
		go peer.sendMessage(Message{RequestVote, Vote{port, term}}) // send Request Vote
	}
}

/*
	Performs said operation on data
*/
func performOperation(op Command) {
	fmt.Println("[X] Executed Command!")

	switch op.Operation {
	case SET:
		data = op.Data
	case ADD:
		data += op.Data
	}
	fmt.Println(op.Operation, op.Data, " --> ", data)
}

/*
	Routine To Send Append Entries to Peers
	Sends after every 3 seconds;
*/
func initHeartBeatMessage() {

	fmt.Println("[4] Init Heartbeat Timer..")
loop:
	for {
		// Sleep
		time.Sleep(3 * time.Second)

		select {
		case <-stopHeartBeat:
			// Stop HeartBeat
			break loop
		// Send Command to peers
		case commandRecv := <-entryQueue_chan:
			fmt.Println("[->] Sending AppendEntriesWithData to peers..")
			for _, peer := range peers {
				go peer.sendMessage(Message{AppendEntryWithData, commandRecv})
			}

		case <-blackout_chan:
			fmt.Println("[->] Sending Blackout to peers..")
			// Choose a partner peer for leader
			for _, peer := range peers {
				go peer.sendMessage(Message{Blackout, peers[0]})
			}
			// Leader only has one peer node
			if len(peers) > 0 {
				blackoutPeers = append(blackoutPeers, peers[0])
			}
			peers = blackoutPeers // Change Peers Pointer

		default:
			fmt.Println("[->] Sending Heartbeat to peers..")
			for _, peer := range peers {
				go peer.sendMessage(Message{AppendEntry, Vote{port, term}})
			}
		}
	}
}

/*
	Returns true if Majority Votes Are Acquired.
	Sends LeaderAppointed Message to all peers if true
*/
func checkMajority() bool {

	if voteCount == MajorityCount { // Majority Acquired
		for _, peer := range peers {
			go peer.sendMessage(Message{LeaderAppointed, port})
		}
		state = Leader
		fmt.Println("[3] Leader elected..")

		return true
	}
	return false
}

/*
	Sends Vote to a Candidate Only if Node Term
	is less than Vote Term ( Meaning node hasn't casted
	vote yet )
*/
func sendVote(vote Vote) bool {

	// TODO contingency cases ??
	fmt.Println("[2] Election Term: ", vote.Term)
	sender := Peer{vote.Sender}
	voteCasted := false
	electionTerm := vote.Term
	if term < electionTerm { // node hasn't voted for this term yet
		term = electionTerm
		fmt.Println("[->] Sending vote...")
		// TODO peer object Find
		//var conn net.Conn
		go sender.sendMessage(Message{GiveVote, vote})
		voteCasted = true
	} else {
		fmt.Println("[2] Node already sent vote..")
		// Leader already exists in network with higher term
		if state == Leader {
			go sender.sendMessage(Message{LeaderAppointed, port})

			go sender.sendMessage(Message{UpdateLog, UpdateLogStruct{entryLog, lastLogIndex, data}})
		}
	}
	return voteCasted
}

/*
	Set Leader
	Finds The Peer object of the Leader and Assigns it.
*/
func setLeader(peerLeader string) {

	for _, peer := range peers {
		if peer.Port == peerLeader {
			leader = &peer
			fmt.Println("[3] Setting leader: ", peer.Port)
			state = Follower
			break
		}
	}

}

// May Cause SERIOUS Deadlocks ... Use with caution.
// Don't Even think of making it routine.
// GETS STUCK IF THE TIMER IS ALREADY EXPIRED AND CHANNEL IS EMPTY, t.Stop() fails to stop
// Refer: https://golang.org/pkg/time/
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		<-t.C // drain timer
	}
	t.Reset(d)
}

// TODO creating new timers everytime .. any better way??
/*
	Routine to handle every Communication
*/
func initElectionRoutineHandler() {

	// Sleep for enabling node boot ?
	//electionTimer = time.NewTimer(5 * time.Second) // TODO Random
	electionTimer = time.NewTimer(time.Duration(rand.Intn(interval)+offset) * time.Second)

	// TODO Figure out to make a blank object -->>POINTERS<<--
	// throws invalid memory access for timer.C
	heartbeatTimer = time.NewTimer(0 * time.Second)
	<-heartbeatTimer.C // drain
loop:
	for {

		select {

		// Election Timer has expired
		// Start New Election With +1 Term
		// Become Candidate And Request For Vote
		// Reset Election Timer
		case <-electionTimer.C:

			fmt.Println("[0] Election Timer expired..")
			term++
			voteCount = 1
			// Does state matter ??
			switch state {
			case Follower:
				go startElection()
			case Candidate:
				go startElection()
			}

			// Reset Election Timer
			fmt.Println("[1] Election Timer Reset..")
			electionTimer = time.NewTimer(time.Duration(rand.Intn(interval)+offset) * time.Second)

		// A node sent a vote
		case vote := <-addVote:

			fmt.Println("[-] Vote Count: ", voteCount, "  Term: ", term)
			// Check if recv msg of vote is of the same term
			if vote.Term == term {
				voteCount++

				fmt.Println("[-] Vote Count: ", voteCount, "  Term: ", term)

				// If majority achieved
				if checkMajority() {

					// Stop Election Timer
					fmt.Println("[3] Stopping Election Timer..")
					if !electionTimer.Stop() {
						<-electionTimer.C // drain timer
					}

					go initHeartbeatRoutineHandler() // switch to heartbeat routine
					go initHeartBeatMessage()        // Start sending AppendEntries Mesages
					break loop                       // exit from election routine

				} else {
					// Reset Election Timer
					fmt.Println("[2] Reseting Election timer..")
					resetTimer(electionTimer, time.Duration(rand.Intn(interval)+offset)*time.Second)
				}
			}

		// A node requested for vote
		case vote := <-voteRequest_chan:
			fmt.Println("[] Vote Request by ", vote.Sender)
			// Send response to vote only if you are a Follower & No Leader exists in network
			//			if state == Follower {

			go sendVote(vote)
			fmt.Println("[2] Reseting Election timer..")
			resetTimer(electionTimer, time.Duration(rand.Intn(interval)+offset)*time.Second)

			//			} else {
			/*
					go sendVote(vote)

					if stopOnce == false {
						fmt.Println("[5] Stopping Heartbeat Timer..")
						if !heartbeatTimer.Stop() {
							<-heartbeatTimer.C // drain timer
						}
						fmt.Println("[5] Starting Election Timer..")
						electionTimer = time.NewTimer(time.Duration(rand.Intn(interval)+offset) * time.Second)
						stopOnce = true
					} else {
						fmt.Println("[2] Reseting Election timer..")
						resetTimer(electionTimer, time.Duration(rand.Intn(interval)+offset)*time.Second)
					}

				}
			*/

			//			}

		// A Leader Has been Appointed
		case peer := <-appointLeader:

			//firstLeader := leader == nil
			setLeader(peer)

			// Stop Election Timer
			fmt.Println("[3] Election Timer Stopped..")
			if !electionTimer.Stop() {
				<-electionTimer.C // drain timer
			}
			heartbeatTimer = time.NewTimer(time.Duration(rand.Intn(interval)+offset) * time.Second)
			go initHeartbeatRoutineHandler() // switch to heartbeat routine
			break loop                       // exit from election routine
			//stopOnce = false
		}

	}

}

func initHeartbeatRoutineHandler() {

loop:
	for {
		select {

		// Heartbeat Timer has expired
		// Start New Election with +1 Term
		// Start Election Timer
		case <-heartbeatTimer.C:
			// Append Entry Not Recieved not recvied ..
			fmt.Println("[4] Heartbeat Timer expired..")

			term++
			voteCount = 1
			go startElection()

			// Start Election Timer
			fmt.Println("[1] Election Timer Start..")
			electionTimer = time.NewTimer(time.Duration(rand.Intn(interval)+offset) * time.Second)

			// switch to election routine
			go initElectionRoutineHandler()
			break loop
			// exit the function

		case vote := <-voteRequest_chan:
			fmt.Println("[] Vote Request by ", vote.Sender)
			// Send response to vote only if you are a Follower & No Leader exists in network
			//			if state == Follower {

			if voteCasted := sendVote(vote); voteCasted {
				fmt.Println("[5] Stopping Heartbeat Timer..")
				if !heartbeatTimer.Stop() {
					<-heartbeatTimer.C // drain timer
				}

				fmt.Println("[5] Starting Election Timer..")
				electionTimer = time.NewTimer(time.Duration(rand.Intn(interval)+offset) * time.Second)
				go initElectionRoutineHandler()
				break loop
			}

			//			}
		// A Leader has sent append entries message [Vote] .. Send Response
		case leaderRecv := <-appendEntry_chan:
			// TODO compare leaders ??
			// Append entry recv from new leader
			if leaderRecv.Term > term {

				term = leaderRecv.Term
				// Step down if leader
				if state == Leader {
					state = Follower
					stopHeartBeat <- true // Close initHeartbeatMessages
				}
				// Set new leader
				setLeader(leaderRecv.Sender)

				// Request Updated Log
				fmt.Println("[-] Requesting Updated Log..")
				go leader.sendMessage(Message{RequestUpdateLog, Peer{port}})

				heartbeatTimer = time.NewTimer(time.Duration(rand.Intn(interval)+offset) * time.Second)

			} else if leaderRecv.Term == term {
				go leader.sendMessage(Message{AppendEntryResponse, Peer{port}})
				// reset heartbeat timer ( may or maynot be drained )
				fmt.Println("[4] Reseting Heartbeat Timer..")
				resetTimer(heartbeatTimer, time.Duration(rand.Intn(interval)+offset)*time.Second)
			}
			// Less than can be a stray append entry -- Ignore

			resetTimer(heartbeatTimer, time.Duration(rand.Intn(interval)+offset)*time.Second)

		// A Follower received AppendEntrywithData
		case changeLogRecv := <-appendEntryData_chan:
			// TODO check for duplicate  ??
			fmt.Println("---->Recv:", changeLogRecv.Index, "Saved:", lastLogIndex)
			if changeLogRecv.Index > lastLogIndex {
				lastLogIndex = changeLogRecv.Index
			}
			// New Log Entry
			if changeLogRecv.Commit == false {
				entryLog[changeLogRecv.Index] = changeLogRecv
				// Resend to verify
				go leader.sendMessage(Message{AppendEntryResponseWithData, changeLogRecv})
			} else {
				obj := entryLog[changeLogRecv.Index]
				obj.Commit = true
				entryLog[changeLogRecv.Index] = obj
				go performOperation(obj.CommandExec) // Commit Changes to data
			}

			for key, value := range entryLog {
				fmt.Println("Key:", key, "Value:", value)
			}
			// reset heartbeat timer ( may or maynot be drained )
			fmt.Println("[4] Reseting Heartbeat Timer..")
			resetTimer(heartbeatTimer, time.Duration(rand.Intn(interval)+offset)*time.Second)

		// A leader received AppendEntryResponseWithData
		case recv := <-appendEntryResponseData_chan:
			obj := entryLog[recv.Index]
			obj.Contributors += 1
			if obj.Commit == false { // If not already committed

				if obj.Contributors == MajorityCount { // Check Majority

					obj.Commit = true
					entryQueue_chan <- obj               // Add in queue to be sent with append entry message
					go performOperation(obj.CommandExec) // Commit Changes on leader node
				}
			}

			entryLog[recv.Index] = obj

		// Leader Has Opted To Black Out with a Peer
		case peerToRemove := <-goDark_chan:
			for _, peer := range peers {
				if peer.Port != peerToRemove.Port && peer.Port != leader.Port {
					blackoutPeers = append(blackoutPeers, peer)
				}
			}
			peers = blackoutPeers

			fmt.Println("[4] Reseting Heartbeat Timer..")
			resetTimer(heartbeatTimer, time.Duration(rand.Intn(interval)+offset)*time.Second)
		}
	}
}

/*
	Function to handle requests recieved
	args := conn - Connection from request is received

	TODO drop irrelevent messages instead of pushing to channels ?
*/
func handleConnectionRequest(conn net.Conn) {

	// Receive message from connection
	msg, ok := recvMessage(conn)
	if !ok {
		return
	}
	fmt.Println("[<-] Received Message: ", msg.MessageType)

	switch msg.MessageType {

	// A Candidate Node has requested for Vote
	// Message Data : Vote{}
	case RequestVote:
		voteRequest_chan <- msg.Data.(Vote)

	// A Candidate Node has received a Vote from a Follower Node
	// Message Data : Vote{}
	case GiveVote:
		addVote <- msg.Data.(Vote)

	// A Candidate Node has been appointed as Leader
	// Message Data : String [Port of Leader]
	case LeaderAppointed:
		appointLeader <- msg.Data.(string)

	// A Leader has sent Append Entry Message
	// Message Data : Peer{} [Leader]
	case AppendEntry:
		//fmt.Println("Sending IN CHANNEL")
		appendEntry_chan <- msg.Data.(Vote)

	// A Leader has recieved Append Entry Response from Follower
	// Message Data : Peer{} [Follower]
	case AppendEntryResponse:
		fmt.Println("[4] Append Entry Response From ", (msg.Data.(Peer)).Port)
		//TODO what to do ?

	// A client has send a log entry command
	case LogCommand:
		if state == Leader {
			lastLogIndex += 1
			newLogEntry := ChangeLog{msg.Data.(Command), lastLogIndex, false, 1}
			entryLog[lastLogIndex] = newLogEntry
			entryQueue_chan <- newLogEntry
		} else {
			fmt.Println("[E] Node Not Leader")
		}

	// A Follower Recieved Append Entry with Data
	case AppendEntryWithData:
		appendEntryData_chan <- msg.Data.(ChangeLog)

	// A Leader Recieved Append Entry Response with Data
	case AppendEntryResponseWithData:
		appendEntryResponseData_chan <- msg.Data.(ChangeLog)

	// Simulate Network Partition.
	case Blackout:
		if state == Leader {
			blackout_chan <- msg.Data.(Peer)
		} else {
			// Remove leader from array of peers
			goDark_chan <- msg.Data.(Peer)
		}
	// Heal Network Partition
	case LightsOn:
		peers = originalPeers
		if state == Leader {
			for _, peer := range peers {
				go peer.sendMessage(Message{LightsOn, Peer{port}})
			}
		}
	// A Node has requested updated log
	case RequestUpdateLog:
		peer := msg.Data.(Peer)
		go peer.sendMessage(Message{UpdateLog, UpdateLogStruct{entryLog, lastLogIndex, data}})

		// A Node has received updated log
	case UpdateLog:
		recv := msg.Data.(UpdateLogStruct)
		data = recv.CurrentData
		entryLog = recv.Log
		lastLogIndex = recv.LastIndex
		for key, value := range entryLog {
			fmt.Println("Key:", key, "Value:", value)
		}

	}

}

/*
	Initializes Listening to Port
	args := port - port to listen

*/
func initListen(port string) {
	// Listen at Port

	fmt.Println("-->Listening at port " + port)
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		fmt.Println("Error IN Listen")
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		// Call Routine to handle requests
		go handleConnectionRequest(conn)

	}

}

func main() {

	// Seed Random
	rand.Seed(time.Now().UTC().UnixNano())

	// Command Line Args
	ListenPortPtr := flag.String("l", "2020", "port to listen")
	flag.Parse()
	DialPorts := flag.Args() // Peers' ports

	// Register Interfaces
	gob.Register(Peer{})
	gob.Register(Vote{})
	gob.Register(Command{})
	gob.Register(ChangeLog{})
	gob.Register(UpdateLogStruct{})

	// Init Channels
	voteRequest_chan = make(chan Vote)
	addVote = make(chan Vote)
	appointLeader = make(chan string)
	stopHeartBeat = make(chan bool)
	appendEntry_chan = make(chan Vote)
	appendEntryData_chan = make(chan ChangeLog)
	appendEntryResponseData_chan = make(chan ChangeLog, 10)
	entryQueue_chan = make(chan ChangeLog, 5)

	blackout_chan = make(chan Peer)
	goDark_chan = make(chan Peer)
	letThereBeLight_chan = make(chan bool)

	entryLog = make(map[int]ChangeLog)

	port = *ListenPortPtr

	initPeers(DialPorts)

	go initElectionRoutineHandler()

	initListen(port)
}

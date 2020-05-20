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

var (
	port     string
	peers    []Peer
	leader   *Peer = nil
	stopOnce bool  = false

	state     StateEnum = Follower
	term      int       = 0
	voteCount int       = 1

	addVote          chan Vote
	voteRequest_chan chan Vote
	appointLeader    chan string
	stopHeartBeat    chan bool
	appendEntry_chan chan Peer
)
var (
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
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
		peers = append(peers, Peer{port})
	}

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

	gob.Register(Peer{})
	gob.Register(Vote{})

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

	gob.Register(Peer{})
	gob.Register(Vote{})

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
	Routine To Send Append Entries to Peers
	Sends after every 3 seconds;
*/
func initHeartBeatMessage() {

	fmt.Println("[4] Init Heartbeat Timer..")
	for {
		// Sleep
		time.Sleep(3 * time.Second)

		select {
		case <-stopHeartBeat:
			// Stop HeartBeat
		default:
			fmt.Println("[->] Sending Heartbeat to peers..")
			for _, peer := range peers {
				go peer.sendMessage(Message{AppendEntry, Peer{port}})

			}
		}
	}
}

/*
	Returns true if Majority Votes Are Acquired.
	Sends LeaderAppointed Message to all peers if true
*/
func checkMajority() bool {

	if voteCount == (len(peers)+1)/2+1 { // Majority Acquired
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
func sendVote(vote Vote) {

	// TODO contingency cases ??
	fmt.Println("[2] Election Term: ", vote.Term)

	electionTerm := vote.Term
	if term < electionTerm { // node hasn't voted for this term yet
		term = electionTerm
		fmt.Println("[->] Sending vote...")
		// TODO peer object Find
		//var conn net.Conn
		sender := Peer{vote.Sender}
		go sender.sendMessage(Message{GiveVote, vote})
	} else {
		fmt.Println("[2] Node already sent vote..")
	}

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
func initRoutineHandler() {

	// Sleep for enabling node boot ?
	//electionTimer = time.NewTimer(5 * time.Second) // TODO Random
	electionTimer = time.NewTimer(time.Duration(rand.Intn(5)+6) * time.Second)

	// TODO Figure out to make a blank object -->>POINTERS<<--
	// throws invalid memory access for timer.C
	heartbeatTimer = time.NewTimer(0 * time.Second)
	<-heartbeatTimer.C // drain

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
			electionTimer = time.NewTimer(time.Duration(rand.Intn(5)+6) * time.Second)

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
			electionTimer = time.NewTimer(time.Duration(rand.Intn(5)+6) * time.Second)

		// A node sent a vote
		case vote := <-addVote:
			// Check if recv msg of vote is of the same term

			fmt.Println("[-] Vote Count: ", voteCount, "  Term: ", term)

			if vote.Term == term {
				voteCount++

				fmt.Println("[-] Vote Count: ", voteCount, "  Term: ", term)

				// If majority achieved
				if checkMajority() {

					go initHeartBeatMessage() // Start sending AppendEntries Mesages

					// Stop Election Timer
					fmt.Println("[3] Stopping Election Timer..")
					if !electionTimer.Stop() {
						<-electionTimer.C // drain timer
					}

				} else {
					// Reset Election Timer
					fmt.Println("[2] Reseting Election timer..")
					resetTimer(electionTimer, time.Duration(rand.Intn(5)+6)*time.Second)
				}
			}

		// A node requested for vote
		case vote := <-voteRequest_chan:
			fmt.Println("[] Vote Request by ", vote.Sender)

			// Send response to vote only if you are a Follower & No Leader exists in network

			if state == Follower {

				if leader == nil {
					go sendVote(vote)
					fmt.Println("[2] Reseting Election timer..")
					resetTimer(electionTimer, time.Duration(rand.Intn(5)+6)*time.Second)
				} else {
					go sendVote(vote)

					if stopOnce == false {
						fmt.Println("[5] Stopping Heartbeat Timer..")
						if !heartbeatTimer.Stop() {
							<-heartbeatTimer.C // drain timer
						}
						fmt.Println("[5] Starting Election Timer..")
						electionTimer = time.NewTimer(time.Duration(rand.Intn(5)+6) * time.Second)
						stopOnce = true
					} else {
						fmt.Println("[2] Reseting Election timer..")
						resetTimer(electionTimer, time.Duration(rand.Intn(5)+6)*time.Second)
					}

				}

			}

		// A Leader Has been Appointed
		case peer := <-appointLeader:

			//firstLeader := leader == nil
			go setLeader(peer)

			// Stop Election Timer
			fmt.Println("[3] Election Timer Stopped..")
			if !electionTimer.Stop() {
				<-electionTimer.C // drain timer
			}

			heartbeatTimer = time.NewTimer(time.Duration(rand.Intn(5)+7) * time.Second)
			stopOnce = false
		// A Leader has sent append entries message .. Send Response
		case leaderRecv := <-appendEntry_chan:
			// TODO compare leaders ??
			go leaderRecv.sendMessage(Message{AppendEntryResponse, Peer{port}})

			// reset heartbeat timer ( may or maynot be drained )
			fmt.Println("[4] Reseting Heartbeat Timer..")
			resetTimer(heartbeatTimer, time.Duration(rand.Intn(5)+7)*time.Second)
		}

	}

}

/*
	Function to handle requests recieved
	args := conn - Connection from request is received
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
		appendEntry_chan <- msg.Data.(Peer)

	// A Leader has recieved Append Entry Response from Follower
	// Message Data : Peer{} [Follower]
	case AppendEntryResponse:
		fmt.Println("[4] Append Entry Response From ", (msg.Data.(Peer)).Port)
		//TODO what to do ?
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

	// Init Channels
	voteRequest_chan = make(chan Vote)
	addVote = make(chan Vote)
	appointLeader = make(chan string)
	stopHeartBeat = make(chan bool)
	appendEntry_chan = make(chan Peer)

	port = *ListenPortPtr
	go initListen(port)

	go initPeers(DialPorts)

	initRoutineHandler()

}

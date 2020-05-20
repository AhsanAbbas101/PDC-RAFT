package main

type MessageTypeEnum int

const (
	RequestVote MessageTypeEnum = iota
	GiveVote
	LeaderAppointed
	HeartBeat
	AppendEntry
	AppendEntryResponse
)

func (d MessageTypeEnum) String() string {
	return [...]string{"RequestVote", "GiveVote", "LeaderAppointed", "HeartBeat", "AppendEntry", "AppendEntryResponse"}[d]
}

type StateEnum int

const (
	Follower StateEnum = iota
	Candidate
	Leader
)

func (d StateEnum) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[d]
}

package main

type MessageTypeEnum int

const (
	RequestVote MessageTypeEnum = iota
	GiveVote
	LeaderAppointed
	HeartBeat
	AppendEntry
	AppendEntryResponse
	AppendEntryWithData
	AppendEntryResponseWithData
	LogCommand
	Blackout
	LightsOn
)

func (d MessageTypeEnum) String() string {
	return [...]string{"RequestVote",
		"GiveVote",
		"LeaderAppointed",
		"HeartBeat",
		"AppendEntry",
		"AppendEntryResponse",
		"AppendEntryWithData",
		"AppendEntryResponseWithData",
		"LogCommand",
		"Blackout",
		"LightsOn"}[d]
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

type OperationEnum int

const (
	SET OperationEnum = iota
	GET
	ADD
	SUBTRACT
)

func (d OperationEnum) String() string {
	return [...]string{"SET", "GET", "ADD", "SUBTRACT"}[d]
}

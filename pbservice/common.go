package pbservice

import "hash/fnv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrInconsistent   = "ErrInconsistent"
	ErrForwardError = "ErrForwardError"
	ErrDeadServer = "ErrDeadServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	RPCSource string
	OPID string // operation ID, client+randomeID

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

type RollbackArgs struct {
	Data SyncedTable
	SeenOPID SyncedTable
}

type RollbackReply struct {
	Err Err
}

type ForwardArgs struct {
	Data SyncedTable
	SeenOPID SyncedTable
}

type ForwardReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// The following structs are deprecated.
type RecoverArgs struct {
}

type RecoverReply struct {
	Err Err
}
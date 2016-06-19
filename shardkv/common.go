package shardkv

import (
	"hash/fnv"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	IsUpdatingConfig = "IsUpdatingConfig"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id string
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id string
}

type GetReply struct {
	Err   Err
	Value string
}

type FetchDataArgs struct {
	ConfigNum int
}

type FetchDataReply struct {
	Err	Err
	Data map[string]string
	SeenOp map[string]string
}

type UpdateConfigArgs struct {
	MostRecentConfigNum	int
}

type UpdateConfigReply struct {
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

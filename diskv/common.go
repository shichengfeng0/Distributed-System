package diskv

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

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id	  string
}

type PutAppendReply struct {
	Err Err
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
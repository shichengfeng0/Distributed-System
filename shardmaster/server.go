package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import (
	"math/rand"
	"strconv"
	"time"
)

const (
	Join = "JOIN"
	Leave = "LEAVE"
	Move = "MOVE"
	Query = "QUERY"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	seqDone int
}

type Op struct {
	// Your data here.
	Id	string
	OpType string
	JoinArgs *JoinArgs
	LeaveArgs *LeaveArgs
	MoveArgs *MoveArgs
	QueryArgs *QueryArgs
	QueryReply *QueryReply
}

func (sm *ShardMaster) doOperation(op Op) {
	curSeq := sm.seqDone

	done := false
	for !done {
		curSeq++
		sm.px.Start(curSeq, op)

		to := 10 * time.Millisecond
		for {
			decided, decidedValue := sm.px.Status(curSeq)
			if decided {
				decidedOp := decidedValue.(Op)

				sm.doJob(decidedOp)

				if decidedOp.Id == op.Id {
					done = true
				}

				sm.px.Done(curSeq)
				sm.seqDone = curSeq

				break
			}

			if !done {
				time.Sleep(to)
				if to < 10 * time.Second {
					to *= 2
				}
			}
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	#three
	sm.mu.Lock()
	defer sm.mu.Unlock()

	newOp := Op{}
	newOp.Id = strconv.Itoa(rand.Int())
	newOp.OpType = Join
	newOp.JoinArgs = args

	sm.doOperation(newOp)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {

	sm.mu.Lock()
	defer sm.mu.Unlock()

	//	curSeq := sm.seqDone
	newOp := Op{}
	newOp.Id = strconv.Itoa(rand.Int())
	newOp.OpType = Leave
	newOp.LeaveArgs = args

	sm.doOperation(newOp)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	#three
	sm.mu.Lock()
	defer sm.mu.Unlock()

	newOp := Op{}
	newOp.Id = strconv.Itoa(rand.Int())
	newOp.OpType = Move
	newOp.MoveArgs = args

	sm.doOperation(newOp)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	#three
	sm.mu.Lock()
	defer sm.mu.Unlock()

	curSeq := sm.seqDone
	newOp := Op{}
	newOp.Id = strconv.Itoa(rand.Int())
	newOp.OpType = Query
	newOp.QueryArgs = args
	newOp.QueryReply = reply

	done := false

	for !done {
		curSeq++

		sm.px.Start(curSeq, newOp)

		to := 10 * time.Millisecond

		for !done {
			decided, decidedValue := sm.px.Status(curSeq)
			if decided {
				decidedOp := decidedValue.(Op)

				sm.doJob(decidedOp)

				if decidedOp.Id == newOp.Id {
					done = true
					reply.Config = decidedOp.QueryReply.Config
				}

				sm.px.Done(curSeq)
				sm.seqDone = curSeq

				break
			}

			if !done {
				time.Sleep(to)
				if to < 10 * time.Second {
					to *= 2
				}
			}
		}
	}

	return nil
}

func (sm *ShardMaster) doJob(op Op) {
	if op.OpType == Join {
		sm.doJoin(op)
	} else if op.OpType == Leave {
		sm.doLeave(op)
	} else if op.OpType == Move {
		sm.doMove(op)
	} else if op.OpType == Query {
		sm.doQuery(op)
	}
}


func (sm *ShardMaster) doJoin(op Op) {
	oldConfig := sm.configs[len(sm.configs) - 1]

	newConfig := Config{}

	// Update config num.
	newConfig.Num = oldConfig.Num + 1

	// Rebalance shards.
	newConfig.Shards = balanceShards(oldConfig, op)

	// Update server for current group id.
	newConfig.Groups = copyMap(oldConfig.Groups)
	serversForCurrentGroup := copyStringSlice(oldConfig.Groups[op.JoinArgs.GID])
	for _, newServer := range op.JoinArgs.Servers {
		if !arrayContains(serversForCurrentGroup, newServer) {
			serversForCurrentGroup = append(serversForCurrentGroup, newServer)
		}
	}
	newConfig.Groups[op.JoinArgs.GID] = serversForCurrentGroup

	sm.configs = append(sm.configs, newConfig)
}

func balanceShards(oldConfig Config, op Op) [NShards]int64 {
	if op.OpType == Join {
		return joinBalance(oldConfig.Shards, op.JoinArgs.GID)
	} else {
		// Leave operation.
		return leaveBalance(oldConfig.Shards, op.LeaveArgs.GID, oldConfig.Groups)
	}
}

// rebalance shards after leave operation
func leaveBalance(oldShards [NShards]int64, leaveGID int64, groups map[int64][]string) [NShards]int64 {
	balancedShards := copyInt64Slice(oldShards)
	track := make(map[int64][]int)

	groupNum := 0

	leaveGIDSeen := false

	for groupId, _ := range groups {
		track[groupId] = []int{}
	}

	for index, shardGID := range balancedShards {
		if shardGID == leaveGID {
			leaveGIDSeen = true
		}

		track[shardGID] = append(track[shardGID], index)
	}

	if !leaveGIDSeen {
		return balancedShards
	}

	if groupNum == 1 {
		balancedShards = [NShards]int64{}
		return balancedShards
	}

	if indexArray, ok := track[leaveGID]; ok {
		for _, replaceIndex := range indexArray {
			minGID := findShardIndexWithLeastGroup(track, leaveGID)
			balancedShards[replaceIndex] = minGID
			track[minGID] = append(track[minGID], replaceIndex)
		}
	}

	return balancedShards
}

// Find  group id with least occurance.
func findShardIndexWithLeastGroup(track map[int64][]int, leaveGID int64) int64 {
	minGroupNum := NShards + 1
	minGID := int64(-1)
	for gid, indices := range track {
		if gid != leaveGID && len(indices) < minGroupNum {
			minGroupNum = len(indices)
			minGID = gid
		}
	}

	return  minGID
}

// rebalance shards after join operation
func joinBalance(oldShards [NShards]int64, joinGID int64) [NShards]int64 {
	balancedShards := copyInt64Slice(oldShards)

	track := make(map[int64][]int)

	groupNum := 0

	newGIDSeen := false

	for index, shardGID := range balancedShards {
		if shardGID == joinGID {
			newGIDSeen = true
		}

		if _, ok := track[shardGID]; !ok {
			track[shardGID] = []int{}
			groupNum++
		} else {
		}
		track[shardGID] = append(track[shardGID], index)
	}

	if newGIDSeen {
		return balancedShards
	}

	avg := NShards / (groupNum + 1)

	if _, ok := track[0]; ok {
		avg = NShards
	}

	for i := 1; i <= avg; i++ {
		maxGID, index := findShardIndexWithMostGroup(track)
		balancedShards[index] = joinGID
		track[maxGID] = track[maxGID][1:]
	}

	return balancedShards
}

// Find  group id with most occurance.
func findShardIndexWithMostGroup(track map[int64][]int) (int64, int) {
	maxGroupNum := -1
	maxGroupIndex := -1
	maxGID := int64(-1)
	for gid, indices := range track {
		if len(indices) > maxGroupNum {
			maxGroupNum = len(indices)
			maxGroupIndex = indices[0]
			maxGID = gid
		}
	}

	return  maxGID, maxGroupIndex
}

func copyStringSlice(slice []string) []string {
	sliceCopy := []string{}
	for _, element := range slice {
		sliceCopy = append(sliceCopy, element)
	}
	return sliceCopy
}

func copyInt64Slice(slice [NShards]int64) [NShards]int64 {
	sliceCopy := [NShards]int64{}
	for i, element := range slice {
		sliceCopy[i] = element
	}
	return sliceCopy
}

func copyMap(oldMap map[int64][]string) map[int64][]string {
	newMap := make(map[int64][]string)
	for k, v := range oldMap {
		newMap[k] = v
	}

	return newMap
}

func arrayContains(array []string, target string) bool{
	for _, value := range array {
		if value == target {
			return true
		}
	}
	return false
}

func (sm *ShardMaster) doLeave(op Op) {
	oldConfig := sm.configs[len(sm.configs) - 1]

	newConfig := Config{}

	// Update config num.
	newConfig.Num = oldConfig.Num + 1

	// Rebalance shards.
	newConfig.Shards = balanceShards(oldConfig, op)

	// Update server for current group id.
	newConfig.Groups = copyMap(oldConfig.Groups)
	delete(newConfig.Groups, op.LeaveArgs.GID)

	sm.configs = append(sm.configs, newConfig)
	//println(newConfig.Shards)
}

func (sm *ShardMaster) doMove(op Op) {
	oldConfig := sm.configs[len(sm.configs) - 1]

	newConfig := Config{}

	// Update config num.
	newConfig.Num = oldConfig.Num + 1

	// Update server for current group id.
	newConfig.Groups = copyMap(oldConfig.Groups)
	newConfig.Shards = copyInt64Slice(oldConfig.Shards)
	newConfig.Shards[op.MoveArgs.Shard] = op.MoveArgs.GID

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) doQuery(op Op){
	if op.QueryArgs.Num == -1 {
		op.QueryReply.Config = sm.configs[len(sm.configs) - 1]
	} else {
		op.QueryReply.Config = sm.configs[op.QueryArgs.Num]
	}
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}

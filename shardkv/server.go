package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import (
	"shardmaster"
	"strconv"
)

const Debug = 0
const (
	Get = "GET"
	Put = "PUT"
	UpdateConfig = "UPDATE_CONFIG"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Id string
	OperationType string
	GetArgs	*GetArgs
	PutArgs *PutArgs
	GetReply *GetReply
	PutReply *PutReply
	UpdateConfigArgs *UpdateConfigArgs
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	data	map[string]string
	seenOp	map[string]string
	seqDone int
	config  shardmaster.Config
	isUpdatingConfig bool

	// Data and seen operation with specific config num.
	// Used by other servers to update their configs.
	dataHistroy map[int](map[string]string)
	seenOpHistory map[int](map[string]string)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	#three
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.dead {
		return nil
	}

	newOp := Op{}
	newOp.Id = args.Id
	newOp.OperationType = Get
	newOp.GetArgs = args
	newOp.GetReply = reply

	kv.doOperation(&newOp)
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	#three
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.dead {
		return nil
	}

	newOp := Op{}
	newOp.Id = args.Id
	newOp.OperationType = Put
	newOp.PutArgs = args
	newOp.PutReply = reply

	kv.doOperation(&newOp)
	return nil
}

func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.dead {
		return
	}

	mostRecentConfig := kv.sm.Query(-1)

	if mostRecentConfig.Num != kv.config.Num {
		op := Op{}
		op.OperationType = UpdateConfig
		op.UpdateConfigArgs = &UpdateConfigArgs{mostRecentConfig.Num}
		kv.doOperation(&op)
	}
}

// Fetch data and operation seen with given config num.
func (kv *ShardKV) FetchData(fetchDataArgs *FetchDataArgs, fetchDataReply *FetchDataReply) error {
	if data, ok := kv.dataHistroy[fetchDataArgs.ConfigNum]; ok {
		fetchDataReply.Data = copyMapData(data)

		if seenOp, ok2 := kv.seenOpHistory[fetchDataArgs.ConfigNum]; ok2 {
			fetchDataReply.SeenOp = copyMapData(seenOp)
			fetchDataReply.Err = OK
		}
	}
	return nil
}

// Update the config to the most recent one.
func (kv *ShardKV) updateConfig(op *Op) {
	mostRecentConfigNum := op.UpdateConfigArgs.MostRecentConfigNum

	for mostRecentConfigNum != kv.config.Num {
		// Config has been changed.
		// Find the next config, we will update one by one.
		kv.isUpdatingConfig = true

		// Record the data with corresponding config.
		storeData := make(map[string]string)
		for k, v := range kv.data {
			if kv.config.Shards[key2shard(k)] == kv.gid {
				storeData[k] = v
			}
		}
		kv.dataHistroy[kv.config.Num] = storeData

		// Record the operation seen and will be used to update other server to prevent duplicate operation.
		kv.seenOpHistory[kv.config.Num] = copyMapData(kv.seenOp)

		// Find the next config.
		nextConfig := kv.sm.Query(kv.config.Num + 1)
		oldConfig := kv.config

		if oldConfig.Num != 0 {
			for shardIndex, oldGID := range oldConfig.Shards {
				newGID := nextConfig.Shards[shardIndex]
				if newGID != oldGID {
					// Current Shard needs to be transfered.
					if newGID == kv.gid {
						// Fetch data from other servers.
						fetchDone := false
						for !fetchDone {
							for _, oldServer := range oldConfig.Groups[oldGID] {
								// Fetch data and operation seen from corresponding server.
								fetchDataArgs := &FetchDataArgs{oldConfig.Num}
								var fetchDataReply FetchDataReply
								ok := call(oldServer, "ShardKV.FetchData", fetchDataArgs, &fetchDataReply)
								if ok && fetchDataReply.Err == OK {
									kv.addData(fetchDataReply.Data)
									kv.addSeenOp(fetchDataReply.SeenOp)
									fetchDone = true
									break
								} else {
//									println("fetch failed!!!")
								}
							}
						}
					} else {
						// Nothing should be done if the current server is not related with changed shards.
					}
				}
			}
		}

		kv.config = nextConfig
	}
}

// Add given seen operation to current seen operation.
func (kv *ShardKV) addSeenOp(newSeenOp map[string]string) {
	for k, v := range newSeenOp {
		kv.seenOp[k] = v
	}
}

func copyMapData(data map[string]string) map[string]string {
	newData := make(map[string]string)

	for k, v := range data {
		newData[k] = v
	}

	return newData
}

// Do Get, Put and Update config operation.
func (kv *ShardKV) doOperation(op *Op) {
	if kv.dead {
		return
	}

	if value, ok := kv.seenOp[op.Id]; ok {
		// If operation is already done.
		if op.OperationType == Get {
			op.GetReply.Value = value
			op.GetReply.Err = OK
		} else if op.OperationType == Put {
			op.PutReply.Err = OK
			if op.PutArgs.DoHash {
				op.PutReply.PreviousValue = value
			}
		} else if op.OperationType == UpdateConfig {
			// Nothing should be done.
		}
		return
	}

	curSeq := kv.seqDone

	done := false

	for !done {
		curSeq++
		kv.px.Start(curSeq, *op)

		to := 10 * time.Millisecond
		for {
			decided, decidedValue := kv.px.Status(curSeq)
			if decided {
				decidedOp := decidedValue.(Op)

				kv.doCorrespondingOperation(&decidedOp)

				if decidedOp.Id == op.Id {
					done = true
					if op.OperationType == Get {
						op.GetReply.Value = decidedOp.GetReply.Value
						op.GetReply.Err = decidedOp.GetReply.Err
					} else if op.OperationType == Put {
						op.PutReply.Err = decidedOp.PutReply.Err
						if op.PutArgs.DoHash {
							op.PutReply.PreviousValue = decidedOp.PutReply.PreviousValue
						}
					} else if op.OperationType == UpdateConfig {

					}
				}

				kv.px.Done(curSeq)
				kv.seqDone = curSeq

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

func (kv *ShardKV) doCorrespondingOperation(op *Op) {
	if op.OperationType == Get {
		kv.doGet(op)
	} else if op.OperationType == Put {
		kv.doPut(op)
	} else if op.OperationType == UpdateConfig {
		kv.updateConfig(op)
	}
}

func (kv *ShardKV) doGet(op *Op) {
	getArgs := op.GetArgs
	getReply := op.GetReply

	// Check whether current get operation is sent to the correct group, since the config could change.
	if kv.config.Shards[key2shard(getArgs.Key)] != kv.gid {
		getReply.Err = ErrWrongGroup
		return
	}

	if value, ok := kv.data[getArgs.Key]; ok {
		getReply.Value = value
		getReply.Err = OK
		kv.seenOp[op.Id] = value
	} else {
		getReply.Err = ErrNoKey
	}
}

func (kv *ShardKV) doPut(op *Op) {
	putArgs := op.PutArgs
	putReply := op.PutReply

	// Check whether current put operation is sent to the correct group, since the config could change.
	if kv.config.Shards[key2shard(putArgs.Key)] != kv.gid {
		putReply.Err = ErrWrongGroup
		return
	}

	if putArgs.DoHash {
		prevValue := ""
		oldValue, ok := kv.data[putArgs.Key]
		if ok {
			prevValue = oldValue
		}
		newValue := strconv.Itoa(int(hash(prevValue + putArgs.Value)))

		kv.data[putArgs.Key] = newValue
		putReply.PreviousValue = prevValue

		kv.seenOp[op.Id] = prevValue
	} else {
		kv.data[putArgs.Key] = putArgs.Value

		kv.seenOp[op.Id] = "PUT DONE"
	}

	putReply.Err = OK
}

// Add data to current shardkv server.
func (kv *ShardKV) addData(dataToBeAdded map[string]string) {
	for k, v := range dataToBeAdded {
		kv.data[k] = v
	}
}

// Remove data from current shardkv.
func (kv *ShardKV) removeData(dataToBeRemoved map[string]string) {
	for k, _ := range dataToBeRemoved {
		delete(kv.data, k)
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	kv.seqDone = 0
	kv.data = make(map[string]string)
	kv.seenOp = make(map[string]string)
	kv.dataHistroy = make(map[int](map[string]string))
	kv.seenOpHistory = make(map[int](map[string]string))
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}

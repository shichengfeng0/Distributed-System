package kvpaxos

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
	"time"
	"strconv"
	"bufio"
	"strings"
)

const Debug = 0

const Get = "GET"
const Put = "PUT"
const Separator = "$DEADBEEF_WHATANICESEPERATOR$"
const LineSeparator = "$LIVEBEEF_WHATANICELINESEPERATOR$"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id string // Server address + random number
	Type string // Type Get/Put
	Key string
	Value string
	DoHash bool
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seqDone		int // last seq known to current paxos server.
	seenOp 		map[string]string // Keep track of seen operation.
	data        map[string]string // Data map.
	seqToOpID	map[int]string
	logFile		string // Log filename.
	minFreedSeq	int // Can forget about everything before this seq
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	#three
	kv.mu.Lock()
	defer kv.mu.Unlock(); kv.clearMemory()

	value, seen := kv.seenOp[args.OpId]
	if seen {
		reply.Value = value
		reply.Err = OK
		//println("Seen in Get -> Returning value: ", value)
		return nil
	} else {
		// Flag to indicate whehter current operation is done or not.
		doneOp := false

		curSeq := kv.seqDone
		for !doneOp {
			curSeq++

			// Operation to be done.
			newOp := Op{args.OpId, Get, args.Key, "", false}

			kv.px.Start(curSeq, newOp)

			// Wait for current seq to be decided.
			to := 10 * time.Millisecond
			for {
				decided, decidedValue := kv.px.Status(curSeq)
				if decided {
					// If current seq is decided.

					prevValue := "" // To be put into seenOp.

					decidedOp := decidedValue.(Op)
					// Check whether current
					if decidedOp.Id == newOp.Id {
						// If current operation is selected.
						value, ok := kv.data[decidedOp.Key]
						if ok {
							//println("Got value ", value)
							reply.Value = value
							reply.Err = OK
							prevValue = value
						} else {
							reply.Err = ErrNoKey
							prevValue = ""
						}
						doneCurrentOp := kv.performLogOperation(args.OpId, curSeq)
						// Write to log.
						if !doneCurrentOp {
							kv.writeToLog(ConstructLogInfo(decidedOp.Id, curSeq, decidedOp.Key, decidedOp.Value, false, prevValue, Get))
						}

						// Set the doneOp flag to true
						doneOp = true

					} else {
						kv.performLogOperation(args.OpId, curSeq)
						// Other paxos server has already declared for current seq.
						if decidedOp.Type == Get {
							// Get Operation.
							prevValue = kv.data[decidedOp.Key]
						} else {
							// Put operation.
							prevValue = ""

							oldValue, seen := kv.data[decidedOp.Key]
							if seen {
								prevValue = oldValue
							}

							if decidedOp.DoHash {
								newValue := strconv.Itoa(int(hash(prevValue + decidedOp.Value)))
								kv.data[decidedOp.Key] = newValue
							} else {
								kv.data[decidedOp.Key] = decidedOp.Value
							}
						}
					}

					// Update seenOp.
					kv.seenOp[decidedOp.Id] = prevValue
					kv.seqToOpID[curSeq] = decidedOp.Id

					// Update doneSeq
					kv.seqDone = curSeq

					// Update done for paxos.
					kv.px.Done(curSeq)

					// Continue to try next seq.
					break
				}

				if !doneOp {
					time.Sleep(to)
					if to < 10 * time.Second {
						to *= 2
					}
				}
			}
		}
	}

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	#three
	kv.mu.Lock()
	defer kv.mu.Unlock(); kv.clearMemory()


	value, seen := kv.seenOp[args.OpId]
	if seen {
		reply.PreviousValue = value
		reply.Err = OK
		return nil
	} else {
		// Flag to indicate whehter current operation is done or not.
		doneOp := false

		curSeq := kv.seqDone
		for !doneOp {
			curSeq++

			// Operation to be done.
			newOp := Op{args.OpId, Put, args.Key, args.Value, args.DoHash}

			kv.px.Start(curSeq, newOp)

			// Wait for current seq to be decided.
			to := 10 * time.Millisecond
			for {
				decided, decidedValue := kv.px.Status(curSeq)
				if decided {
					// If current seq is decided.

					prevValue := "" // To be put into seenOp.

					decidedOp := decidedValue.(Op)
					// Check whether current
					if decidedOp.Id == newOp.Id {
						// If current operation is selected.
						oldValue, ok := kv.data[decidedOp.Key]
						if ok {
							prevValue = oldValue
							reply.PreviousValue = oldValue
						}

						// See if it is doHash
						if decidedOp.DoHash {
							newValue := strconv.Itoa(int(hash(prevValue + decidedOp.Value)))
							kv.data[decidedOp.Key] = newValue
						} else {
							kv.data[decidedOp.Key] = decidedOp.Value
						}

						reply.Err = OK

						doneCurrentOp := kv.performLogOperation(args.OpId, curSeq)
						// Write to log.
						if !doneCurrentOp {
							kv.writeToLog(ConstructLogInfo(decidedOp.Id, curSeq, decidedOp.Key, decidedOp.Value, false, prevValue, Put))
						}

						// Set the doneOp flag to true
						doneOp = true
					} else {
						kv.performLogOperation(args.OpId, curSeq)
						// Other paxos server has already declared for current seq.
						if decidedOp.Type == Get {
							// Get Operation.
							prevValue = kv.data[decidedOp.Key]
						} else {
							// Put operation.
							prevValue = ""

							oldValue, seen := kv.data[decidedOp.Key]
							if seen {
								prevValue = oldValue
							}

							if decidedOp.DoHash {
								newValue := strconv.Itoa(int(hash(prevValue + decidedOp.Value)))
								kv.data[decidedOp.Key] = newValue
							} else {
								kv.data[decidedOp.Key] = decidedOp.Value
							}
						}
					}

					// Update seenOp.
					kv.seenOp[decidedOp.Id] = prevValue
					kv.seqToOpID[curSeq] = decidedOp.Id

					// Update doneSeq
					kv.seqDone = curSeq

					// Update done for paxos.
					kv.px.Done(curSeq)

					// Continue to try next seq.
					break
				}

				if !doneOp {
					time.Sleep(to)
					if to < 10 * time.Second {
						to *= 2
					}
				}
			}
		}
	}
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	os.Remove(kv.logFile)
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.seenOp = make(map[string]string)
	kv.seqToOpID = make(map[int]string)
	kv.seqDone = 0
	kv.logFile = "KVPaxos-Log"

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

// Clear unnecessary memory for paxos.
func (kv *KVPaxos) clearMemory() {
	kv.px.Min()
//	minSeq := kv.px.Min()
//	kv.minFreedSeq = minSeq
//	for seq, OpId := range kv.seqToOpID {
//		if seq < minSeq {
//			fmt.Printf("delete seenOpId with seq = %s, minSeq = %s\n", seq, minSeq)
//			delete(kv.seenOp, OpId)
//		}
//	}
//
//	for i:= 1; i < minSeq; i++ {
//		fmt.Printf("delete seqToOpId with minSeq = %s\n", minSeq)
//		delete(kv.seqToOpID, i)
//	}
}

// Write log.
func (kv *KVPaxos) writeToLog(data string) bool{
	// Log the put operation into log file.
	f, err := os.OpenFile(kv.logFile, os.O_APPEND|os.O_RDWR|os.O_CREATE , 0777)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(data); err != nil {
		panic(err)
	}
	return true
}

// Construct log information.
func ConstructLogInfo(operationID string, seq int, key string, value string, doHash bool, prevValue string, opType string) string {
	return operationID + Separator + strconv.Itoa(seq) + Separator + opType + Separator + key + Separator + value + Separator + strconv.FormatBool(doHash) + Separator + prevValue + LineSeparator
}

// Catch up operation with log.
func (kv *KVPaxos) performLogOperation(curOPID string, seqDone int) bool{
	doneBySomeoneElse := false

	// Check whether log recover file exists or not.
	if _, err := os.Stat(kv.logFile); os.IsNotExist(err) {
		// No need to recover since there isn't a recover log file yet.
		return doneBySomeoneElse
	}

	logFile, err := os.OpenFile(kv.logFile, os.O_RDONLY, 0777)
	if err != nil {
		return doneBySomeoneElse
	}

	defer logFile.Close()

	if stat, _ := logFile.Stat(); stat.Size() == 0 {
		// Log file is empty.
		return doneBySomeoneElse
	}

	// Read recover log line by line.

	scanner := bufio.NewScanner(logFile)

	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		if i:= strings.Index(string(data), LineSeparator); i >= 0 {
			return i + len(LineSeparator), data[0:i], nil
		}

		if atEOF {
			return len(data), data, nil
		}

		return
	}


	curToDo := kv.minFreedSeq


	scanner.Split(split)
	for scanner.Scan() {
		line := scanner.Text()
		lineArgs := strings.Split(line, Separator)

//		fmt.Printf("line is %s\n", line)

		opid := lineArgs[0]
		seq := lineArgs[1]
		opType := lineArgs[2]
		key := lineArgs[3]
		value := lineArgs[4]
		//println("============= line is ", line)
		doHash, _ := strconv.ParseBool(lineArgs[5])
		preValue := ""
		if len(lineArgs) >= 7 {
			preValue = lineArgs[6]
		}

		intSeq, _ := strconv.Atoi(seq)

//		fmt.Printf("opid = %s, seq = %s, opType = %s, key = %s, value = %s, doHash = %s, preValue = %s \n", opid, seq, opType, key,value,doHash, preValue)

		if opid == curOPID {
			doneBySomeoneElse = true
//			return doneBySomeoneElse
		}

		if curToDo == intSeq {
			if _, seen := kv.seenOp[opid]; !seen && intSeq < seqDone && intSeq >= kv.minFreedSeq {
				//			println("IN HERE!!!!!! ", opid)
				fmt.Printf("opid = %s, seq = %s, opType = %s, minFreed = %s\n", opid, seq, opType, kv.minFreedSeq)

				if opType == Put {
					if doHash {
						newValue := strconv.Itoa(int(hash(preValue + value)))
						kv.data[key] = newValue
						kv.seenOp[opid] = preValue
					} else {
						//println("Put data key, value :", key, ", ", value)
						kv.data[key] = value
						kv.seenOp[opid] = preValue
					}
				} else if opType == Get {
					kv.seenOp[opid] = preValue
				} else {
					println("FUCKED")
					// Do nothing
				}
			}
			curToDo++
		} else {

		}



	}
	return doneBySomeoneElse
//	return false
}
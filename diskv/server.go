package diskv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import (
	"strconv"
	"strings"
	"bufio"
	"bytes"
)

const  (
	Debug = 0
	Get = "GET"
	Put = "Put"
	UpdateConfig = "UPDATE_CONFIG"
	Append = "Append"
)

const Separator = ",,"
const SavedStateFilename = "SavedState"
const SavedOperationFilename = "SavedOperation"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Id string
	OperationType string
	GetArgs	*GetArgs
	PutAppendArgs *PutAppendArgs
	GetReply *GetReply
	PutAppendReply *PutAppendReply
	UpdateConfigArgs *UpdateConfigArgs
}


type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

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

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

func (kv *DisKV) encState() string {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.config.Num)
	e.Encode(kv.seqDone)
	return string(w.Bytes())
}

// decode a string originally produced by enc() and
// return the original values.
func (kv *DisKV) decState(buf string) (configNum int, seqDone int) {
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	var x1 int
	var x2 int
	d.Decode(&x1)
	d.Decode(&x2)
	return x1, x2
}

// Get directory for file used to keep track of server state(includes max sequence done and config number)
func (kv *DisKV) getSavedStateDir() string {
	d := kv.dir + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// Get file name that used to keep track of operation state, which is used by paxos.
func (kv *DisKV) getSavedOperationFilename() string {
	return kv.getSavedStateDir() + SavedOperationFilename
}

// Get file name that used to keep track of server state.
func (kv *DisKV) getSavedStateFilename() string {
	return kv.getSavedStateDir() + SavedStateFilename
}

// Write operation log to disk, which will be used to recover paxos.
func (kv *DisKV) writeOperationToLog(op Op, seqNum int) error {
	fullname := kv.getSavedOperationFilename()
	tempname := fullname + "-tmp" + strconv.Itoa(int(nrand() + 1))
	content := kv.formatOperationLog(op, seqNum)

//	f, err := os.OpenFile(fullname, os.O_APPEND | os.O_RDWR | os.O_CREATE , 0777)
//	f.Close()

//	oldContent, err := ioutil.ReadFile(fullname)

	f, err := os.OpenFile(tempname, os.O_APPEND | os.O_RDWR | os.O_CREATE , 0777)
	if err != nil {
		panic(err)
	}
//	f.Write(oldContent)

	f, err = os.OpenFile(tempname, os.O_APPEND | os.O_RDWR | os.O_CREATE , 0777)
	defer f.Close()

	if _, err = f.WriteString(content); err != nil {
		return err
	}

	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}

	return nil
}

// Format the operation log content saved to disk.
func (kv *DisKV) formatOperationLog(op Op, seq int) string {
	opId := op.Id
	opType := op.OperationType
	content := ""
	if op.OperationType == Get {
		content += op.GetArgs.Key
	} else if op.OperationType == Append || op.OperationType == Put {
		content += op.PutAppendArgs.Key + Separator + op.PutAppendArgs.Value
	} else if op.OperationType == UpdateConfig {
		content += strconv.Itoa(op.UpdateConfigArgs.MostRecentConfigNum)
	}

	return opId + Separator + strconv.Itoa(seq) + Separator + opType + Separator + content + "\n"
}

// Convert []Op to []interface{}.
func convertOpListToInterfaceList(ops []Op) []interface{}{
	result := []interface{}{}
	for _, op := range ops {
		result = append(result, op)
	}
	return result
}

// Get server sequence number, operation done, a
func (kv *DisKV) getRecoverInfo() (seqNums []int, ops []Op, maxSeqDone int) {
	// Check whether log recover file exists or not.
	recoverFilename := kv.getSavedOperationFilename()
	if _, err := os.Stat(recoverFilename); os.IsNotExist(err) {
		// No need to recover since there isn't a recover log file yet.
		return []int{}, []Op{}, 0
	}

	logFile, err := os.OpenFile(recoverFilename, os.O_RDONLY, 0777)
	if err != nil {
		return []int{}, []Op{}, 0
	}

	defer logFile.Close()

	// Check whether log recover file is empty or not.
	if stat, _ := logFile.Stat(); stat.Size() == 0 {
		return []int{}, []Op{}, 0
	}

	// Note: seqList and opList should have the same length, since they are corresponding to each other.
	seqList := []int{}
	opList := []Op{}
	maxSeqDone = 0

	// Read recover log line by line.
	scanner := bufio.NewScanner(logFile)
	for scanner.Scan() {
		line := scanner.Text()
		lineArgs := strings.Split(line, Separator)
		opID := lineArgs[0]
		seq, _ := strconv.Atoi(lineArgs[1])
		opType := lineArgs[2]


		if seq > maxSeqDone {
			maxSeqDone = seq
		}

		op := Op{}
		op.OperationType = opType
		op.Id = opID

		if opType == Get {
			key := lineArgs[3]
			op.GetArgs = &GetArgs{}
			op.GetArgs.Key = key
			op.GetArgs.Id = opID
			var getReply GetReply
			op.GetReply = &getReply

			if op.GetArgs.Key != "dummy-key" {
				// Do not add seen op if that's dummy opertaion.
				kv.seenOp[opID] = "seenOP"
			}
		} else if opType == UpdateConfig {
			mostRecentConfigNum := lineArgs[3]
			op.UpdateConfigArgs = &UpdateConfigArgs{}
			op.UpdateConfigArgs.MostRecentConfigNum, _ = strconv.Atoi(mostRecentConfigNum)

			kv.seenOp[opID] = "seenOP"
		} else if opType == Append || opType == Put {
			key := lineArgs[3]
			value := lineArgs[4]
			op.PutAppendArgs = &PutAppendArgs{}
			op.PutAppendArgs.Key = key
			op.PutAppendArgs.Value = value
			op.PutAppendArgs.Op = opType
			var putReply PutAppendReply
			op.PutAppendReply = &putReply
			kv.seenOp[opID] = "seenOP"
		} else {
			panic("Invalid operation type!")
		}

		seqList = append(seqList, seq)
		opList = append(opList, op)
	}
	return seqList, opList, maxSeqDone
}

// Get server state.
func (kv *DisKV) getSavedState() (configNum int, seqDone int) {
	fullname := kv.getSavedStateFilename()
	content, err := ioutil.ReadFile(fullname)

	if err == nil {
		return kv.decState(string(content))
	} else {
		return 0, 0
	}
}

// Saved server state.
func (kv *DisKV) saveState() error {
	fullname := kv.getSavedStateFilename()
	tempname := fullname + "-tmp"
	content := kv.encState()

	f, err := os.OpenFile(tempname, os.O_RDWR | os.O_CREATE , 0777)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(content); err != nil {
		return err
	}

	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}

	return nil
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// Replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.dead == 1 {
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

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	#three
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.dead == 1 {
		return nil
	}

	newOp := Op{}
	newOp.Id = args.Id
	newOp.OperationType = args.Op
	newOp.PutAppendArgs = args
	newOp.PutAppendReply = reply

	kv.doOperation(&newOp)
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *DisKV) tick() {
	#three
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.dead == 1 {
		return
	}

	// Force the paxos to catch up.
	kv.sendDummyOpToPaxos()

	mostRecentConfig := kv.sm.Query(-1)

	if mostRecentConfig.Num != kv.config.Num {
		op := Op{}
		op.OperationType = UpdateConfig
		op.UpdateConfigArgs = &UpdateConfigArgs{mostRecentConfig.Num}
		kv.doOperation(&op)
	}
}


// Fetch data and operation seen with given config num.
func (kv *DisKV) FetchData(fetchDataArgs *FetchDataArgs, fetchDataReply *FetchDataReply) error {
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
func (kv *DisKV) updateConfig(op *Op) {
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
								ok := call(oldServer, "DisKV.FetchData", fetchDataArgs, &fetchDataReply)
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
func (kv *DisKV) addSeenOp(newSeenOp map[string]string) {
	for k, v := range newSeenOp {
		kv.seenOp[k] = v
	}
}

// Copy map.
func copyMapData(data map[string]string) map[string]string {
	newData := make(map[string]string)

	for k, v := range data {
		newData[k] = v
	}

	return newData
}

// Do Get, Put and Update config operation.
func (kv *DisKV) doOperation(op *Op) {
	if kv.dead == 1 {
		return
	}

	if value, ok := kv.seenOp[op.Id]; ok {
		// If operation is already done.
		if op.OperationType == Get {
			op.GetReply.Value = value
			op.GetReply.Err = OK
		} else if op.OperationType == Put {
			op.PutAppendReply.Err = OK
			//			if op.PutArgs.DoHash {
			//				op.PutReply.PreviousValue = value
			//			}
		} else if op.OperationType == Append {
			fmt.Printf("REJECTED KEY/VALUE: %v/%v", op.PutAppendArgs.Key, op.PutAppendArgs.Value)
			op.PutAppendReply.Err = OK
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
					} else if op.OperationType == Put || op.OperationType == Append{
						op.PutAppendReply.Err = decidedOp.PutAppendReply.Err
						//						if op.PutArgs.DoHash {
						//							op.PutReply.PreviousValue = decidedOp.PutReply.PreviousValue
						//						}
					} else if op.OperationType == UpdateConfig {

					}
				}

				kv.px.Done(curSeq)
				kv.seqDone = curSeq

				kv.writeOperationToLog(decidedOp, curSeq)
				kv.saveState()

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

func (kv *DisKV) doCorrespondingOperation(op *Op) {
	if op.OperationType == Get {
		kv.doGet(op)
	} else if op.OperationType == Put || op.OperationType == Append{
		kv.doPut(op)
	} else if op.OperationType == UpdateConfig {
		kv.updateConfig(op)
	}
}

func (kv *DisKV) doGet(op *Op) {
	getArgs := op.GetArgs
	getReply := op.GetReply

	shard := key2shard(getArgs.Key)
	// Check whether current get operation is sent to the correct group, since the config could change.
	if kv.config.Shards[shard] != kv.gid {
		getReply.Err = ErrWrongGroup
		return
	}

	if value, ok := kv.data[getArgs.Key]; ok {
		getReply.Value = value
		getReply.Err = OK

		if op.GetArgs.Key != "dummy-key" {
			kv.seenOp[op.Id] = value
		}
	} else {
		getReply.Err = ErrNoKey
	}
}

func (kv *DisKV) doPut(op *Op) {
	putArgs := op.PutAppendArgs
	putReply := op.PutAppendReply

	shard := key2shard(putArgs.Key)
	// Check whether current put operation is sent to the correct group, since the config could change.
	if kv.config.Shards[shard] != kv.gid {
		putReply.Err = ErrWrongGroup
		return
	}

	if op.OperationType == Append {
		prevValue := ""
		//oldValue, err := kv.fileGet(shard, putArgs.Key)
		oldValue, ok := kv.data[putArgs.Key]

		if ok {
			prevValue = oldValue
		}
		newValue := prevValue + putArgs.Value
		kv.data[putArgs.Key] = newValue
		kv.filePut(shard, putArgs.Key, newValue)
		kv.seenOp[op.Id] = "APPEND DONE"
	} else {
		kv.data[putArgs.Key] = putArgs.Value
		kv.filePut(shard, putArgs.Key, putArgs.Value)
		kv.seenOp[op.Id] = "PUT DONE"
	}
	putReply.Err = OK
}

// Used force paxos to catch up if it falls behind.
func (kv *DisKV) sendDummyOpToPaxos() {
	args := &GetArgs{}
	args.Key = "dummy-key"
	args.Id = "DUMMY_GET" + strconv.Itoa(nrandInt() + 1)
	var reply GetReply

	newOp := Op{}
	newOp.Id = args.Id
	newOp.OperationType = Get
	newOp.GetArgs = args
	newOp.GetReply = &reply

	kv.doOperation(&newOp)
}

// Add data to current shardkv server.
func (kv *DisKV) addData(dataToBeAdded map[string]string) {
	for k, v := range dataToBeAdded {
		kv.data[k] = v
		//shard := key2shard(k)
		//kv.filePut(shard, k, v)
	}
}

// Remove data from current shardkv.
func (kv *DisKV) removeData(dataToBeRemoved map[string]string) {
	for k, _ := range dataToBeRemoved {
		delete(kv.data, k)
	}
}


// tell the server to shut itself down.
// please don't change these two functions.
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// Your initialization code here.
	kv.seqDone = 0
	kv.data = make(map[string]string)
	kv.seenOp = make(map[string]string)
	kv.dataHistroy = make(map[int](map[string]string))
	kv.seenOpHistory = make(map[int](map[string]string))
	// Don't call Join().

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	if restart {
		configNum, seqDone := kv.getSavedState()
		kv.config = kv.sm.Query(configNum)
		kv.seqDone = seqDone

		shards := kv.config.Shards
		for i, v := range shards {
			if v == kv.gid {
				newData := kv.fileReadShard(i)
				for key, value := range newData {
					kv.data[key] = value
					//					println("RECOVERED KEY/VALUE: %v, %v", key, value)
				}
			}
		}
	}

	if restart {
		seqList, opList, maxSeqDone := kv.getRecoverInfo()
		kv.px.RecoverSeqInstance(seqList, convertOpListToInterfaceList(opList))
		kv.seqDone = maxSeqDone
		defer kv.tick()
	}

	// log.SetOutput(os.Stdout)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.
	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}

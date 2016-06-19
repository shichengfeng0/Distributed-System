package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import (
	"sync"
	"strconv"
	"bufio"
	"strings"
	"errors"
)

// Debugging
const Debug = 0
const LogFile = "Logfile.txt"
const Separator = " "

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type SyncedTable struct {
	mu		sync.Mutex
	Table map[string]string
}

func (table *SyncedTable) Lock() {
	table.mu.Lock()
}

func (table *SyncedTable) Unlock() {
	table.mu.Unlock()
}

func (table *SyncedTable) Copy() SyncedTable {
	newTable := SyncedTable{}
	newTable.Init()
	table.Lock()

	for k, v := range table.Table {
		newTable.Table[k] = v
	}

	table.Unlock()
	return newTable
}


func (table *SyncedTable) Init() {
	table.Table = make(map[string]string)
}

func (table *SyncedTable) Get(args *GetArgs, reply *GetReply){
	table.mu.Lock()
	result, ok := table.Table[args.Key]
	if ok {
		reply.Value = result
		reply.Err = OK
		//testLog("Get key: " + args.Key + " and value: " + reply.Value)
	} else {
		reply.Err = ErrNoKey
	}
	table.mu.Unlock()
}

func (table *SyncedTable) Put(args *PutArgs, reply *PutReply){
	table.mu.Lock()
	if args.DoHash {
		prevValue := ""
		oldValue, ok := table.Table[args.Key]
		if ok {
			prevValue = oldValue
		}
		newValue := strconv.Itoa(int(hash(prevValue + args.Value)))
		table.Table[args.Key] = newValue
		reply.PreviousValue = prevValue

		//testLog("PutHash key, " + args.Key + ", value " + newValue + ", Returned previous value " + prevValue)
	} else {
		table.Table[args.Key] = args.Value
	}
	reply.Err = OK
	table.mu.Unlock()
}

func (table *SyncedTable) SimplePut(key string, value string){
	table.mu.Lock()
	table.Table[key] = value
	table.mu.Unlock()
}

func (table *SyncedTable) SimpleGet(key string) (result string, ok bool){
	table.mu.Lock()
	defer table.mu.Unlock()
	result, ok = table.Table[key]
	return
}

func (pb *PBServer) forwardLock() {
	pb.forwardMu.Lock()
}

func (pb *PBServer) forwardUnlock() {
	pb.forwardMu.Unlock()
}

func (pb *PBServer) getPrimary() (primary string){
	pb.viewMu.Lock()
	primary = pb.curView.Primary
	pb.viewMu.Unlock()
	return
}

func (pb *PBServer) getBackup() (backup string) {
	pb.viewMu.Lock()
	backup = pb.curView.Backup
	pb.viewMu.Unlock()
	return
}

func (pb *PBServer) getView() (view viewservice.View) {
	pb.viewMu.Lock()
	view = pb.curView
	pb.viewMu.Unlock()
	return
}

func (pb *PBServer) setView(view viewservice.View) {
	pb.viewMu.Lock()
	pb.curView = view
	pb.viewMu.Unlock()
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	
	table		SyncedTable
	curView		viewservice.View
	logFile 	string
	seenOPID	SyncedTable // used to keep track of seen operation and its previous value to avoid duplicate execution
	forwardMu	sync.Mutex // Used to lock forward put operation.
	viewMu  	sync.Mutex // Used to lock curView related operation.
	opMu		sync.Mutex // Used to lock operations like put, get.
}

// Test log.
func testLog(message string) {
//	 Log the put operation into log file.
	f, err := os.OpenFile("TestLog.txt", os.O_APPEND|os.O_RDWR|os.O_CREATE , 0777)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(message + "\n"); err != nil {
		panic(err)
	}
}

// Used by testLog.
func convertView(view viewservice.View) string {
	return "view# " + strconv.Itoa(int(view.Viewnum)) + ", primary: " + view.Primary + ", backup: " + view.Backup + ". "
}

// Used to construct log information.
func ConstructLogInfo(operationID string, key string, value string, doHash bool, prevValue string) string {
	return "PUT" + Separator + operationID + Separator + key + Separator + value + Separator + strconv.FormatBool(doHash) + Separator + prevValue + "\n"
}

// Check whether the most-up-to-date primary and back is the same as passed in data.
func (pb *PBServer) checkConsistent(primary string, backup string) bool {
	curView, _ := pb.vs.Get()
	curPrimary := curView.Primary
	curBackup := curView.Backup

	return curPrimary == primary && curBackup == backup
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
//	fmt.Println("Put is called")
	pb.opMu.Lock()
	defer pb.opMu.Unlock()

	if pb.dead {
		reply.Err = ErrWrongServer
		return nil
	}

	// Get the current view based on which this put operation will be executed.
	curView := pb.getView()
	curPrimary := curView.Primary
	curBackup := curView.Backup

	// Get the old data in case we need to rollback.
	oldData := pb.table.Copy()
	oldSeen := pb.seenOPID.Copy()

	if pb.me == curPrimary {
		if prevValue, seen := pb.seenOPID.SimpleGet(args.OPID); seen {
			// Duplicate put
			reply.PreviousValue = prevValue
			reply.Err = OK

			testLog("Put && Primary seen: " + args.OPID + ", returned previous value: " + prevValue + ", with key:" + args.Key)
			return nil
		}

		pb.forwardLock()
		pb.table.Put(args, reply)
		pb.seenOPID.SimplePut(args.OPID, reply.PreviousValue)
		testLog("I am primary: " + pb.me + " Trying to put " + args.OPID + " into seenOPID" )

		if curBackup == "" {
			// No backup
			testLog("Put && ME==PRIMARY -> backup is empty")
		} else {
			// Forward the put operation to backup.
			testLog("I am primary: " + pb.me + " Trying to forward to backup!")

			forwardArgs := &ForwardArgs{}
			forwardArgs.Data = pb.table
			forwardArgs.SeenOPID = pb.seenOPID

			var forwardReply ForwardReply
			ok := call(curBackup, "PBServer.ForwardPut", forwardArgs, &forwardReply)
			ok = ok && forwardReply.Err == OK
			if !ok {
				// If forward put fails, rollback data.
				testLog("Failed to forward to backup: " + curBackup + " ROLL BACK")
				reply.Err = ErrForwardError

				// rollback both primary and backup.
				pb.rollBack(oldData, oldSeen)
				rollbackArgs := &RollbackArgs{oldData, oldSeen}
				var rollbackReply RollbackReply

				// No need to check whether this rollback successed or not since the only
				// case when this rollback fails would be the case that the backup dies, which will
				// be recovered later.
				call(curBackup, "PBServer.Rollback", rollbackArgs, &rollbackReply)
				pb.forwardUnlock()
				return nil
			}
			testLog("Forwarded to backup: " + curBackup)
		}
		testLog("Put && ME==PRIMARY -> current view :" + convertView(curView))
		testLog("Put && ME==PRIMARY -> dohash: " + strconv.FormatBool(args.DoHash) + " key: " + args.Key + " value: " + args.Value + " PreValue: " + reply.PreviousValue)

		// check whether view state has been changed while we are doing this put operation.
		if pb.checkConsistent(curPrimary, curBackup) {
			pb.writeToLog(ConstructLogInfo(args.OPID, args.Key, args.Value, args.DoHash, reply.PreviousValue))
		} else {
			testLog("Roll back")
			pb.rollBack(oldData, oldSeen)
			rbArgs := &RollbackArgs{oldData, oldSeen}
			var rbReply RollbackReply

			// No need to check whether this rollback successed or not since the only
			// case when this rollback fails would be the case that the backup dies, which will
			// be recovered later.
			call(curBackup, "PBServer.Rollback", rbArgs, &rbReply)
		}
		pb.forwardUnlock()
	} else if pb.me == curBackup {
		// Since primary forward the put by calling ForwardPut method, so Backup should do nothing here.
	} else {
		testLog("Put && ME==OTHERS && WRONG SERVER -> currentView: " + convertView(curView) + " ME= " + pb.me)
		reply.Err = ErrWrongServer
		return nil
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.opMu.Lock()
	defer pb.opMu.Unlock()

	if pb.dead {
		reply.Err = ErrWrongServer
		return nil
	}

	if view, ok := pb.vs.Get(); ok &&view.Primary == pb.me {
		pb.table.Get(args, reply)
		testLog("Get from " + pb.me + " with key: " + args.Key + " reply value " + reply.Value)
	} else {
		// Reject, not primary
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) rollBack(oldData SyncedTable, oldSeen SyncedTable) {
	pb.table = oldData
	pb.seenOPID = oldSeen
}

// RPC rollback.
func (pb *PBServer) Rollback(args *RollbackArgs, reply *RollbackReply) error {
	pb.rollBack(args.Data, args.SeenOPID)
	reply.Err = OK
	return nil
}

// RPC forward put.
func (pb *PBServer) ForwardPut(args *ForwardArgs, reply *ForwardReply) error {
	pb.table = args.Data
	pb.seenOPID = args.SeenOPID
	reply.Err = OK
	return nil
}



// ping the viewserver periodically.
func (pb *PBServer) tick() {
	pb.opMu.Lock()
	defer pb.opMu.Unlock()
	if pb.dead {
		return
	}

	newView, ok := pb.vs.Get()
	if ok {
		pb.setView(newView)
		pb.vs.Ping(newView.Viewnum)
	} else {
		// Can not get most recent view so do nothing.
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.table = SyncedTable{}
	pb.table.Init()
	pb.logFile = LogFile
	pb.seenOPID = SyncedTable{}
	pb.seenOPID.Init()

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}

// The following methods are deprecated.
func (pb *PBServer) Recover(args *RecoverArgs, reply *RecoverReply) error {
	pb.opMu.Lock()
	defer pb.opMu.Unlock()

	if pb.dead {
		return nil
	}

	// Clear data.
	pb.table = SyncedTable{}
	pb.table.Init()
	pb.seenOPID = SyncedTable{}
	pb.seenOPID.Init()

	// Check whether log recover file exists or not.
	if _, err := os.Stat(pb.logFile); os.IsNotExist(err) {
		// No need to recover since there isn't a recover log file yet.
		reply.Err = OK
		return nil
	}

	logFile, err := os.OpenFile(pb.logFile, os.O_RDONLY, 0777)
	if err != nil {
		return err
	}

	defer logFile.Close()

	// Check whether log recover file is empty or not.
	if stat, _ := logFile.Stat(); stat.Size() == 0 {
		return errors.New("Open log file failed")
	}

	// Read recover log line by line.
	scanner := bufio.NewScanner(logFile)
	for scanner.Scan() {
		line := scanner.Text()
		lineArgs := strings.Split(line, Separator)
		opType := lineArgs[0]
		opID := lineArgs[1]
		key := lineArgs[2]
		value := lineArgs[3]
		doHash, err := strconv.ParseBool(lineArgs[4])
		prevValue := ""
		if len(lineArgs) >= 6 {
			prevValue = lineArgs[5]
		}

		if err != nil {
			panic(err)
		}

		if opType == "PUT" {
			// Recover the value into table.
			putArgs := &PutArgs{key, value, doHash, "RECOVER", opID}
			var putReply PutReply
			for putReply.Err != OK {
				pb.table.Put(putArgs, &putReply)
			}

			// Set true for this operation ID in seenTable.
			pb.seenOPID.SimplePut(opID, prevValue)
		}
	}
	reply.Err = OK

	return nil
}

func (pb *PBServer) writeToLog(data string) bool{
	// Log the put operation into log file.
	f, err := os.OpenFile(pb.logFile, os.O_APPEND|os.O_RDWR|os.O_CREATE , 0777)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(data); err != nil {
		panic(err)
	}
	return true
}
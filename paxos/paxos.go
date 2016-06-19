package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

				   // Your data here.
	SeqInstanceList	[]*SeqInstance // Record for all the seq instance on current server.
	highestDoneSeq int// Hight done sequence instance.
	executeInstanceBefore bool // Indicate whether this paxos server has done any instance job before
	operationLock sync.Mutex // Lock to all operations.
}

type SeqInstance struct {
	Decided		bool
	Seq			int
	N_A			int
	V_A			interface{}
	N_P			int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	

	if px.dead {
		return
	}

	if seq < px.Min() {
		return
	}
	go func () {
		// Get instance
		px.operationLock.Lock()
		instance, ok := px.findSeqInstance(seq)
		if !ok {
			// Create sequence instance if we don't have one.
			instance = &SeqInstance{false, seq, 0, nil, 0}
			px.SeqInstanceList = append(px.SeqInstanceList, instance)
		}
		px.operationLock.Unlock()

		// Keep proposing if seq instance it no decided.
		for !instance.Decided && !px.dead {
			proposalNum := instance.N_P + 1

			// Counter for prepare OK reply.
			prepareOKCount := 0

			// Keep track of accepted proposal number and corresponding value.
			replyProposalNums := []int{}
			replyValues := []interface{}{}
			for _, peer := range px.peers {
				if px.peers[px.me] != peer {
					// Do RPC call to send prepare to other paxos server.
					args := &PrepareArgs{seq, proposalNum}
					var reply PrepareReply
					ok := call(peer, "Paxos.Prepare", args, &reply)
					ok = ok && reply.Err == OK
					if ok {
						if reply.Decided {
							// If other server is already decided, get out of the foor loop and set local seq
							// instance to be decided.
							instance.V_A = reply.AcceptedValue
							instance.Decided = true
							prepareOKCount = -1 // To skip the accept process later.
							break
						}

						prepareOKCount++
						if reply.AcceptedValue != nil {
							// If there is accepted value returned,
							replyProposalNums = append(replyProposalNums, reply.AcceptedProposalNum)
							replyValues = append (replyValues, reply.AcceptedValue)
						}
					} else {
						// RPC prepare call failed.
					}
				} else {
					// Local prepare.
					prepareOk, decied, acceptedProposalNum, acceptedValue := px.localPrepare(seq, proposalNum)
					if decied {
						prepareOKCount = -1 // To skip the accept later.
						break
					}

					if prepareOk {
						prepareOKCount++
						if acceptedValue != nil {
							replyProposalNums = append(replyProposalNums, acceptedProposalNum)
							replyValues = append (replyValues, acceptedValue)
						}
					}
				}
			}

			if prepareOKCount > len(px.peers) / 2 {
				// If received RPC prepare from majority.
				value := px.findValyeWithHighestProposalNum(replyProposalNums, replyValues)
				if value == nil {
					// If there is no accepted value returned.
					value = v
				}

				// Keep track of accept
				acceptOKCount := 0
				// Send accept
				for _, peer := range px.peers {
					if px.peers[px.me] != peer {
						args := &AcceptArgs{seq, proposalNum, value}
						var reply AcceptReply
						ok := call(peer, "Paxos.Accept", args, &reply)
						ok = ok && reply.Err == OK
						if ok {
							if reply.Decided {
								// If we received a decided flag, stop sending accept to other servers
								// and get the decided value and ready to send decide.
								instance.V_A = reply.DecidedValue
								instance.Decided = true
								acceptOKCount = -1 // To skip the decide later.
								break
							}
							acceptOKCount++
						}
					} else {
						// LOCAL ACCEPT
						acceptOK, decided := px.localAccept(seq, proposalNum, value)
						if decided {
							acceptOKCount = -1
							break
						}

						if acceptOK {
							acceptOKCount++
						}
					}
				}

				if acceptOKCount > len(px.peers) / 2 {
					decidedCounter := 0
					for _, peer := range px.peers {
						if px.peers[px.me] != peer {
							args := &DecidedArgs{seq, proposalNum, value}
							var reply DecidedReply
							ok := call(peer, "Paxos.Decide", args, &reply)
							ok = ok && reply.Err == OK
							if ok {
								if reply.DecidedAlready {
									instance.Decided = true
									instance.V_A = reply.DecidedValue
								}
								decidedCounter++
							}
						} else {
							decidedOK, decidedBefore := px.localDecide(seq, value)
							if decidedBefore {
								break
							}

							if decidedOK {
								decidedCounter++
							}
						}
					}

				} else {

				}
			} else {
				//				println("server ", px.me, " PREPARE NOT RECEIVED FROM MAJORTIY!!! prepareOKCount = ", prepareOKCount)
			}
		}

		for _, peer := range px.peers {
			if px.peers[px.me] != peer {
				args := &DecidedArgs{seq, instance.N_A, instance.V_A}
				var reply DecidedReply
				ok := call(peer, "Paxos.Decide", args, &reply)
				ok = ok && reply.Err == OK
				if ok {
					if reply.DecidedAlready {
						instance.Decided = true
						instance.V_A = reply.DecidedValue
					}
				}
			} else {
				px.localDecide(seq, instance.V_A)
			}
		}
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.operationLock.Lock()
	defer px.operationLock.Unlock()

	px.executeInstanceBefore = true
	if seq > px.highestDoneSeq {
		px.highestDoneSeq = seq
	} else {
		println("Passed in seq is less than current hight done sequence!")
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.operationLock.Lock()
	defer px.operationLock.Unlock()

	max := -1
	for _, seqInstance := range px.SeqInstanceList {
		if seqInstance.Seq > max {
			max = seqInstance.Seq
		}
	}

	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	//	// You code here.
	//	px.operationLock.Lock()
	//	defer px.operationLock.Unlock()

	mineHighestDone := -1
	if px.executeInstanceBefore {
		mineHighestDone = px.highestDoneSeq
	}

	min := mineHighestDone
	counter := 0

	for _, peer := range px.peers {
		if px.peers[px.me] != peer {
			args := &GetHightestDoneSeqArgs{}
			var reply GetHightestDoneSeqReply
			ok := call(peer, "Paxos.GetHightestDoneSeq", args, &reply)
			ok = ok && reply.Err == OK
			if ok {
				counter++
				if reply.HightSeqDone < min {
					min = reply.HightSeqDone
				}
			} else {

			}
		} else {
			counter++
		}
	}

	if counter == len(px.peers) {
		newSeqInstanceList := []*SeqInstance{}
		for _, curInstance := range px.SeqInstanceList {
			if curInstance.Seq > min {
				newSeqInstanceList = append(newSeqInstanceList, curInstance)
			}
		}
		px.SeqInstanceList = newSeqInstanceList

		return min + 1
	} else {
		return -1
	}
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.operationLock.Lock()
	defer px.operationLock.Unlock()

	instance, ok := px.findSeqInstance(seq)
	if !ok {
		return false, nil
	} else {
		if instance.Decided {
			return true, instance.V_A
		} else {
			return false, nil
		}
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

// RPC prepare call.
func (paxos *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	paxos.operationLock.Lock()
	defer paxos.operationLock.Unlock()

	if paxos.dead {
		reply.Err = DEAD_SERVER
		return nil
	}

	instance, ok := paxos.findSeqInstance(args.SeqNum)
	if !ok {
		instance = &SeqInstance{false, args.SeqNum, 0, nil, 0}
		paxos.SeqInstanceList = append(paxos.SeqInstanceList, instance)
	}

	if args.ProposalNum > instance.N_P {
		instance.N_P = args.ProposalNum
		reply.AcceptedProposalNum = instance.N_A
		reply.AcceptedValue = instance.V_A
		reply.Err = OK
		reply.Decided = instance.Decided
	} else {
		reply.HighestProposalSeen = instance.N_P
		reply.Err = REJECT
	}
	return nil
}

// RPC accept call.
func (paxos *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	paxos.operationLock.Lock()
	defer paxos.operationLock.Unlock()

	if paxos.dead {
		reply.Err = DEAD_SERVER
		return nil
	}

	instance, _ := paxos.findSeqInstance(args.SeqNum)

	if instance.Decided {
		reply.Decided = true
		reply.DecidedValue = instance.V_A
		reply.Err = OK
		return nil
	}

	if args.ProposalNum >= instance.N_P && args.ProposalValue != nil {
		instance.N_P = args.ProposalNum
		instance.N_A = args.ProposalNum
		instance.V_A = args.ProposalValue
		reply.Err = OK
	} else {

		reply.HighestProposalSeen = instance.N_P
		reply.Err = REJECT
	}
	return nil
}

// RPC decide call.
func (paxos *Paxos) Decide(args *DecidedArgs, reply *DecidedReply) error {
	paxos.operationLock.Lock()
	defer paxos.operationLock.Unlock()

	if paxos.dead {
		reply.Err = DEAD_SERVER
		return nil
	}

	instance, _ := paxos.findSeqInstance(args.SeqNum)

	if instance.Decided {
		reply.Err = OK
		reply.DecidedAlready = true
		reply.DecidedValue = instance.V_A
	} else {
		instance.V_A = args.DecidedValue
		instance.Decided = true
		reply.Err = OK
	}
	return nil
}

// Get hightest done seq instance for current server.
func (paxos *Paxos) GetHightestDoneSeq(args *GetHightestDoneSeqArgs, reply *GetHightestDoneSeqReply) error {
	//	paxos.operationLock.Lock()
	//	defer paxos.operationLock.Unlock()
	if !paxos.executeInstanceBefore {
		reply.HightSeqDone = -1
	} else {
		reply.HightSeqDone = paxos.highestDoneSeq
	}
	reply.Err = OK
	return nil
}

// Local prepare call.
func (paxos *Paxos) localPrepare(seq int, proposalNum int) (prepareOK bool, decieded bool, acceptedProposalNum int, acceptedValue interface{}) {
	paxos.operationLock.Lock()
	defer paxos.operationLock.Unlock()

	if paxos.dead {
		return false, false, -1, nil
	}

	instance, _ := paxos.findSeqInstance(seq)

	if instance.Decided {
		return false, true, -1, nil
	}

	if proposalNum > instance.N_P {
		instance.N_P = proposalNum
		return true, false, instance.N_A, instance.V_A
	} else {
		return false, false, -1, nil
	}
}

// Local accept call.
func (paxos *Paxos) localAccept(seq int, proposalNum int, proposalValue interface{}) (acceptOk bool, decided bool) {
	paxos.operationLock.Lock()
	defer paxos.operationLock.Unlock()

	if paxos.dead {
		return false, false
	}

	instance, _ := paxos.findSeqInstance(seq)

	if instance.Decided {
		return false, true
	}

	if proposalNum >= instance.N_P {
		instance.N_P = proposalNum
		instance.N_A = proposalNum
		instance.V_A = proposalValue
		return true, false
	} else {
		return false, false
	}

}

// Local decide call.
func (paxos *Paxos) localDecide(seq int, proposedValue interface{}) (decidedOK bool, decidedBefore bool){
	paxos.operationLock.Lock()
	defer paxos.operationLock.Unlock()

	if paxos.dead {
		return false, false
	}

	instance, _ := paxos.findSeqInstance(seq)
	if !instance.Decided {
		instance.V_A = proposedValue
		instance.Decided = true
		return true, false
	} else {
		return false, true
	}
}

// Find seq instance for the given seq number.
func (paxos *Paxos) findSeqInstance(seq int) (*SeqInstance, bool){
	for _, seqInstance := range paxos.SeqInstanceList {
		if seqInstance.Seq == seq {
			return seqInstance, true
		}
	}
	return &SeqInstance{false, seq, 0, nil, 0}, false
}

// Find value with heightest proposal number.
// return nil if the proposal number is empty.
func (paxos *Paxos) findValyeWithHighestProposalNum(nums []int, values []interface{}) (interface{}) {
	maxIndexArr := []int{}
	max := -1
	for _, n := range nums {
		if n > max {
			max = n
		}
	}

	for i, n := range nums {
		if n == max {
			maxIndexArr = append(maxIndexArr, i)
		}
	}

	counter := make(map[interface{}]int)
	for _, n := range maxIndexArr {
		counter[values[n]]++
	}

	var maxV interface{}
	max = -1
	for k, v := range counter {
		if v > max {
			max = v
			maxV = k
		}
	}

	if max == -1 {
		return nil
	} else {
		return maxV
	}
}

func (px *Paxos) RecoverSeqInstance(seqNums []int, values []interface{}) {
	px.operationLock.Lock()
	defer px.operationLock.Unlock()
	highestSeq := -1
	for i := 0; i < len(seqNums); i++ {
		seqNum := seqNums[i]
		if seqNum > highestSeq {
			highestSeq = seqNum
		}
		value := values[i]
		instance := &SeqInstance{true, seqNum, 0, value, 0}
		px.SeqInstanceList = append(px.SeqInstanceList, instance)
	}
	px.highestDoneSeq = highestSeq
}
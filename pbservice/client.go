package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import (
	"math/rand"
	"strconv"
	"time"
)

// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"
// import "math/big"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	client string
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.client = me
	return ck
}

func (ck *Clerk) getClient() string {
	return ck.client
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{key}
	var reply GetReply
	ok := false
	primary := ck.vs.Primary()
	for !ok {
		ok = call(primary, "PBServer.Get", args, &reply)
		ok = ok && reply.Err == OK
		if !ok {
			time.Sleep(viewservice.PingInterval)
			primary = ck.vs.Primary()
		}
	}
	return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	seperator := "$"
	OPID := ck.getClient() + seperator + strconv.Itoa(rand.Int() + 1)
	args := &PutArgs{key, value, dohash, "", OPID}
	var reply PutReply
	ok := false

	primary := ck.vs.Primary()

	for !ok {
		ok = call(primary, "PBServer.Put", args, &reply)
		ok = ok && reply.Err == OK
		if reply.Err != OK {
			time.Sleep(viewservice.PingInterval)
			primary = ck.vs.Primary()
		}
	}
	return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}

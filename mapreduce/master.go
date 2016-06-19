package mapreduce

import "container/list"
import (
	"fmt"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

// Struct with mutex that supports synchronization
type Counter struct {
	mu  sync.Mutex
	value   int
}

func (c *Counter) Increase() {
	c.mu.Lock()
	c.value++
	c.mu.Unlock()
}

func (c *Counter) Value() (result int) {
	c.mu.Lock()
	result = c.value
	c.mu.Unlock()
	return
}

// Synchronized MRDone struct is used to keep track of the whether the whole MapReduce is done or not
// so that waiting threads could stop waiting.
type MRDone struct {
	mu  sync.Mutex
	value   bool
}

func (done *MRDone) Value() (result bool){
	done.mu.Lock()
	result = done.value
	done.mu.Unlock()
	return
}

func (done *MRDone) SetTrue() {
	done.mu.Lock()
	done.value = true
	done.mu.Unlock()
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// Init variables
	var finishedMap Counter
	var finishedReduce Counter
	var mrDone MRDone

	// Fill channel
	for i := 0; i < mr.nMap; i++ {
		mr.mapToDo <- i
	}
	for i := 0; i < mr.nReduce; i++ {
		mr.reduceToDo <- i
	}

	// Run master
	for !mrDone.Value() {
		select {
		case address := <-mr.registerChannel:
			go func() {
				mr.returningWorker <- address
			}()
		case worker := <- mr.returningWorker:
			// as new worker comes in, do job
			if finishedMap.Value() < mr.nMap {
				ExecuteJob(mr, Map, worker, &mrDone, &finishedMap)
			} else if finishedReduce.Value() < mr.nReduce {
				ExecuteJob(mr, Reduce, worker, &mrDone, &finishedReduce)
			} else {
				mrDone.SetTrue()
				close(mr.returningWorker)
				close(mr.mapToDo)
				close(mr.reduceToDo)
			}
		}
	}

	return mr.KillWorkers()
}

// Function that executes job
func ExecuteJob(mr *MapReduce, jobType JobType, worker string, mrDone *MRDone, counter *Counter) {
	// Put operation in go func to avoid blocking.
	go func() {
		jobArgs := &DoJobArgs{}
		jobArgs.File = mr.file
		jobArgs.Operation = jobType
		switch jobType {
		case Map:
			jobArgs.JobNumber = <- mr.mapToDo
			jobArgs.NumOtherPhase = mr.nReduce
		case Reduce:
			jobArgs.JobNumber = <- mr.reduceToDo
			jobArgs.NumOtherPhase = mr.nMap
		}
		replyArgs := &DoJobReply{}
		replyArgs.OK = false

		if mrDone.Value() {
			return
		}

		call(worker, "Worker.DoJob", jobArgs, replyArgs)
		if replyArgs.OK == false {
			// Deal with failure case.
			// Put failure job back into the job queue.
			switch jobType {
			case Map:
				mr.mapToDo <- jobArgs.JobNumber
			case Reduce:
				mr.reduceToDo <- jobArgs.JobNumber
			}
		} else {
			mr.returningWorker <- worker
			counter.Increase()
		}
	}()
}

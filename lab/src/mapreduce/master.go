package mapreduce

import "container/list"
import "fmt"

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

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mapChan := make(chan int, mr.nMap)
	reduceChan := make(chan int, mr.nReduce)

	handOutJob := func(address string, operation JobType, index int) bool {
		jobArgs := &DoJobArgs{mr.file, operation, index, 0}
		reply := &DoJobReply{}
		switch operation {
		case Map:
			jobArgs.NumOtherPhase = mr.nReduce
		case Reduce:
			jobArgs.NumOtherPhase = mr.nMap
		default:
			fmt.Printf("handOutJob: unknown Operation\n")
		}
		return call(address, "Worker.DoJob", jobArgs, reply)
	}

	scheduler := func(okChan chan int, operation JobType, index int) {
		for {
			var workerAddr string
			var isOk bool = false
			select {
			case workerAddr = <-mr.idleChannel:
				isOk = handOutJob(workerAddr, operation, index)
			case workerAddr = <-mr.registerChannel:
				mr.Workers[workerAddr] = &WorkerInfo{workerAddr}
				isOk = handOutJob(workerAddr, operation, index)
			}
			if isOk {
				okChan <- index
				mr.idleChannel <- workerAddr
				return
			}
		}
	}

	for i := 0; i < mr.nMap; i++ {
		go scheduler(mapChan, Map, i)
	}

	for i := 0; i < mr.nMap; i++ {
		id := <-mapChan
		fmt.Printf("Map %d has done\n", id)
	}

	for i := 0; i < mr.nReduce; i++ {
		go scheduler(reduceChan, Reduce, i)
	}

	for i := 0; i < mr.nReduce; i++ {
		id := <-reduceChan
		fmt.Printf("Reduce %d has done\n", id)
	}

	return mr.KillWorkers()
}

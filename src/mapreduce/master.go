package mapreduce

import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
	address string
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

//  process new worker registrations by reading from MapReduce.registerChannel
func ProcessRegister(mr *MapReduce) {
	for {
		address := <-mr.registerChannel
		worker := WorkerInfo{address}
		mr.Workers[address] = &worker
		mr.avaliableChannel <- address
	}
}

func SendJob(mr *MapReduce, jobType JobType, hasDone *int, jobs *list.List) {
	NumOtherPhase := 0
	if jobType == Map {
		NumOtherPhase = mr.nReduce
	} else {
		NumOtherPhase = mr.nMap
	}
	if jobs.Len() > 0 {
		select {
		case id := <-mr.avaliableChannel:
			workerInfo := mr.Workers[id]
			jobNumber := jobs.Remove(jobs.Front()).(int)
			doJobArgs := &DoJobArgs{mr.file, jobType, jobNumber, NumOtherPhase}
			// send calls
			go func() {
				var reply DoJobReply
				isOK := call(workerInfo.address, "Worker.DoJob", doJobArgs, &reply)
				// log.Println("reply: ", reply.OK, " isOK: ", isOK)
				if isOK {
					*hasDone++ // one job is completed
					// DO NOT swap these two lines, cause it will loop forever
					// Another choice: use another channel deal with the response, see:http://goo.gl/2CCGNO
					mr.avaliableChannel <- workerInfo.address
				} else {
					log.Println("worker failed: ", workerInfo.address, jobType)
					delete(mr.Workers, workerInfo.address)
					jobs.PushFront(jobNumber)
				}
			}()
		}
		//id := <-mr.avaliableChannel

	} else {
		//jobs.PushFront(jobNumber)
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	go ProcessRegister(mr)
	hasDone := 0 // current numbers of completed jobs

	// init map jobs
	mapJobs := list.New()
	for i := 0; i < mr.nMap; i += 1 {
		mapJobs.PushBack(i)
	}
	// init reduce jobs
	reduceJobs := list.New()
	for i := 0; i < mr.nReduce; i += 1 {
		reduceJobs.PushBack(i)
	}
	// map
	log.Println("Map phrase begin...")
	log.Println("mr.nMap ", mr.nMap)
	for hasDone != mr.nMap {
		SendJob(mr, Map, &hasDone, mapJobs)
		//log.Println("hasDone ", hasDone)
	}
	log.Println("Map phrase has done...")
	// reduce
	hasDone = 0
	for hasDone != mr.nReduce {
		SendJob(mr, Reduce, &hasDone, reduceJobs)
	}
	log.Println("Reduce phrase had done...")

	return mr.KillWorkers()
}

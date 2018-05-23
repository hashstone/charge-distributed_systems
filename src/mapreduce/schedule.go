package mapreduce

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type taskInfo struct {
	arg    *DoTaskArgs
	worker string
	ok     bool
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// generate DoTaskArgs firstly
	taskArgs := make([]*DoTaskArgs, 0)
	for i := 0; i < ntasks; i++ {
		arg := &DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
		if phase == mapPhase {
			arg.File = mapFiles[i]
		}
		taskArgs = append(taskArgs, arg)
	}

	workers := make([]string, 0)
	for {
		// if have no task to do, break for loop
		if len(taskArgs) == 0 {
			break
		}
		// get active workers
		select {
		case wk := <-registerChan:
			workers = append(workers, wk)
			continue
		default:
		}
		// if there is no workers, wait for it
		if len(workers) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// wait for workers done
		groupNum := len(workers)
		taskInfos := make([]*taskInfo, 0)

		var wg sync.WaitGroup
		for i := 0; i < groupNum; i++ {
			if i >= len(taskArgs) {
				break
			}
			// use pointer slice, or there is something wrong about it!
			// if use instance, there may be member address changed
			taskInfos = append(taskInfos, &taskInfo{
				arg:    taskArgs[i],
				worker: workers[i],
			})
			wg.Add(1)
			go func(info *taskInfo) {
				ok := call(info.worker, "Worker.DoTask", info.arg, nil)
				if !ok {
					log.Printf("call worker:%v:DoTask args:%v fail\n", info.worker, *info.arg)
				}
				info.ok = ok
				wg.Done()
			}(taskInfos[i])
		}
		wg.Wait()

		// accordding worker result to delete taskArgs and workers
		taskArgs = taskArgs[len(taskInfos):]
		workers = workers[len(taskInfos):]
		for _, info := range taskInfos {
			if info.ok {
				// worker ok, reuse it
				workers = append(workers, info.worker)
			} else {
				log.Printf("fail: %v\n", info)
				// task fail, retry it
				taskArgs = append(taskArgs, info.arg)
			}
		}
	}

	fmt.Printf("Schedule: %v done\n", phase)
}

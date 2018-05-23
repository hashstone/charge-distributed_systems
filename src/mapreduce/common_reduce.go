package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// 1. get all kv
	kvArr := make([]KeyValue, 0)
	for mapTask := 0; mapTask < nMap; mapTask++ {
		interFile := reduceName(jobName, mapTask, reduceTask)
		fp, err := os.Open(interFile)
		if err != nil {
			log.Fatalf("open %v fail: %v", interFile, err)
		}
		defer fp.Close()

		interDecoder := json.NewDecoder(fp)
		for {
			var kv KeyValue
			err = interDecoder.Decode(&kv)
			if err != nil {
				//log.Printf("decode fail: %v", err)
				break
			}
			kvArr = append(kvArr, kv)
		}
	}
	if len(kvArr) == 0 {
		return
	}

	// 2. sort kv by key
	sort.Slice(kvArr, func(i, j int) bool { return kvArr[i].Key < kvArr[j].Key })

	// 3. open output file
	of, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		log.Fatalf("open %v fail: %v", outFile, err)
	}
	reduceEncoder := json.NewEncoder(of)

	// 4. reduceF
	preKey := kvArr[0].Key
	values := make([]string, 0)
	for _, kv := range kvArr {
		if kv.Key != preKey {
			okv := KeyValue{Key: preKey, Value: reduceF(preKey, values)}
			err := reduceEncoder.Encode(&okv)
			if err != nil {
				log.Fatalf("encode %v fail", okv)
			}
			preKey = kv.Key
			values = make([]string, 0)
		}
		values = append(values, kv.Value)
	}
	okv := KeyValue{Key: preKey, Value: reduceF(preKey, values)}
	err = reduceEncoder.Encode(&okv)
	if err != nil {
		log.Fatalf("encode %v fail", okv)
	}
}

package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,       // write the output here
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
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
	//     enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	// Read all mrtmp.xxx-m-reduceTaskNumber and write to outFile
	var keyValue []KeyValue
	for i := 0; i < nMap; i++ {
		interName := reduceName(jobName, i, reduceTaskNumber)
		interBody, err := ioutil.ReadFile(interName)
		if err != nil {
			fmt.Println(err)
		}
		dec := json.NewDecoder(strings.NewReader(string(interBody)))
		for {
			var m KeyValue
			if err := dec.Decode(&m); err == io.EOF {
				break
			} else if err != nil {
				fmt.Println(err)
			}
			keyValue = append(keyValue, m)
		}
	}

	//排序及合并,处理后应该类似["0":[""],"1":[""]...]
	var keyValuesMap map[string][]string
	keyValuesMap = make(map[string][]string)
	for _, v := range keyValue {
		if _, ok := keyValuesMap[v.Key]; ok { //若key值已存在，将value添加到[]string中
			keyValuesMap[v.Key] = append(keyValuesMap[v.Key], v.Value)
		} else { //若key值不存在，在map中新建key
			var values []string
			values = append(values, v.Value)
			keyValuesMap[v.Key] = values
		}
	}

	//对每个key调用reduceF,并写入最后的文件
	outputFile, err := os.Create(outFile)
	if err != nil {
		fmt.Println(err)
	}
	enc := json.NewEncoder(outputFile)
	for k, v := range keyValuesMap {
		err := enc.Encode(KeyValue{k, reduceF(k, v)})
		if err != nil {
			fmt.Println(err)
		}
	}
}

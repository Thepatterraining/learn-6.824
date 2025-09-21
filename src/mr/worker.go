package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// KeyValue 列表按 Key 排序所需的类型和方法
// example [{key:a, value:1}, {key:b, value:1}]	
type ByKey []KeyValue

// for sorting by key.
// 实现 sort.Interface
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 注册worker
	string WorkerId := register()

	// 获取Map需要执行的任务
	Task task = getTask(WrokerId)

	
	// 读取文件的内容
	// 调用用户实现的 Map 函数，返回一组 KeyValue
	mapRes := mapf(task.Filename, readFile(task.Filename))
	
	// 按 Key 排序，方便后续把相同 key 的 value 聚集在一起供 Reduce 使用
	sort.Sort(ByKey(mapRes))

	// 输出文件名是固定的 mr-out-0（MIT 6.824 实验要求的输出格式）
	// 其中 X 是 Map 任务编号，Y 是 reduce 任务编号。
	oname := "mr-X-Y"
	intermediateFile, _ := os.Create(oname)
	// 写入中间文件，json格式
	enc := json.NewEncoder(intermediateFile)
  	for _, kv := range mapRes {
    	err := enc.Encode(&kv)
	}

	// reduce读取中间文件的内容

	// 调用reduce函数

	// 结果写入最终文件
	
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func readFile(filename string) string {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 读取整个文件内容到内存（适用于示例/小文件）
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

func register() string {

	// declare an argument structure.
	args := RegisterWorkerRequest{}

	// fill in the argument(s).
	args.Hostname = os.Hostname()
	args.Port = 0

	// declare a reply structure.
	reply := RegisterWorkerResponse{}

	// send the RPC request, wait for the reply.
	call("Master.RegisterWorker", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.WorkerId %v\n", reply.WorkerId)
	return reply.WorkerId
}

func getTask(wrokerId string) Task {

	// declare an argument structure.
	args := GetTaskRequest{}

	// fill in the argument(s).
	args.WorkerId = wrokerId

	// declare a reply structure.
	reply := GetTaskResponse{}

	// send the RPC request, wait for the reply.
	call("Master.getTask", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Task.WorkderId %v\n", reply.Task.WorkderId)
	return reply.Task
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

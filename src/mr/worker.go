package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "os"
import "io/ioutil"
import "encoding/json"


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
	workerId := register()

	// 获取Map需要执行的任务
	for {
		task, nReduce := getTask(workerId)
		// 判断是Map任务还是Reduce任务
		if (task.Type == MapTask) {
			// 执行map任务
			execMap(task, mapf, nReduce)
			// 执行完成 ，通知master
			notifyTaskCompleted(workerId, task.Number)
		} else if (task.Type == ReduceTask) {
			// 执行	Reduce任务
			execReduce(task, reducef)
			// 执行完成 ，通知master
			notifyTaskCompleted(workerId, task.Number)
		} else {
			// 通知master 没有任务了，结束了
			notifyTaskCompleted(workerId, 0)
			return
		}
	}
}

func execReduce(task Task, reducef func(string, []string) string) {
	// 读取中间数据
	intermediateFile, err := os.Open(task.Filename)
	defer intermediateFile.Close()
	if err != nil {
		log.Fatalf("无法打开中间文件 %s: %v", task.Filename, err)
	}
	// 解码
	intermediate := []KeyValue{}
	dec := json.NewDecoder(intermediateFile)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
		break
		}
		intermediate = append(intermediate, kv)
	}

	// shuff
	// 按 Key 排序，方便后续把相同 key 的 value 聚集在一起供 Reduce 使用
	sort.Sort(ByKey(intermediate))

	// 输出文件名是固定的 mr-out-0（MIT 6.824 实验要求的输出格式）
	oname := fmt.Sprintf("mr-out-%d", task.Number)
	ofile, _ := os.Create(oname)
	//
	// 对 intermediate 中每个不同的 key 调用 Reduce，然后把结果写入 mr-out-0
	i := 0
	for i < len(intermediate) {
		// 找到从 i 开始的连续相同 key 的区间 [i, j)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// 收集该 key 对应的所有 values
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 调用用户实现的 Reduce 函数
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		// 按每行 "key value\n" 的格式写入输出文件（与课程/测试要求一致）
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		// 继续下一个不同的 key
		i = j
	}
	ofile.Close()
}

// execMap 执行 Map 任务
// task: 要执行的 Map 任务
// mapf: 用户提供的 Map 函数
// nReduce: Reduce 任务数量，用于分区
func execMap(task Task, mapf func(string, string) []KeyValue, nReduce int) {
	// 读取输入文件内容
	content := readFile(task.Filename)

	// 调用用户实现的 Map 函数，返回一组 KeyValue
	mapRes := mapf(task.Filename, content)
	fmt.Printf("Map 任务 %d 处理文件 %s，生成 %d 个中间键值对\n", task.Number, task.Filename, len(mapRes))
	// 先创建 nReduce 个中间文件
	intermediateFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.Number, i)
		intermediateFile, err := os.Create(filename)
		defer intermediateFile.Close()
		if err != nil {
			log.Fatalf("无法创建中间文件 %s: %v", filename, err)
		}
		intermediateFiles[i] = intermediateFile
	}
	// 将 Map 输出按 Key 哈希分区，分配给不同的 Reduce 任务
  	for _, kv := range mapRes {
		// 获取hash值
		index := ihash(kv.Key)
		// fmt.Printf("ihash 结果：%d\n", index)
		// 输出文件名是固定的 mr-out-0（MIT 6.824 实验要求的输出格式）
		// 其中 X 是 Map 任务编号，Y 是 reduce 任务编号。
		// oname := "mr-"+task.taskNumber+"-"+"0"
		// filename := fmt.Sprintf("mr-%d-%d", task.Number, index % nReduce)
		// intermediateFile, err := os.Create(filename)
		// defer intermediateFile.Close()
		// if err != nil {
		// 	log.Fatalf("无法创建中间文件 %s: %v", filename, err)
		// }
		// 写入中间文件，json格式
		enc := json.NewEncoder(intermediateFiles[index % nReduce])  // 为每个文件创建 JSON 编码器
    	err := enc.Encode(&kv) // 写入中间文件
		if err != nil {
			log.Fatalf("写入中间文件失败: %v", err)
		}
	}
	log.Printf("Map 任务 %d 完成，生成了 %d 个中间文件\n", task.Number, nReduce)

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
	args.Hostname,_ = os.Hostname()
	args.Port = 0

	// declare a reply structure.
	reply := RegisterWorkerResponse{}

	// send the RPC request, wait for the reply.
	call("Master.RegisterWorker", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.WorkerId %v\n", reply.WorkerId)
	return reply.WorkerId
}

func getTask(wrokerId string) (Task, int) {

	// declare an argument structure.
	args := GetTaskRequest{}

	// fill in the argument(s).
	args.WorkerId = wrokerId

	// declare a reply structure.
	reply := GetTaskResponse{}

	// send the RPC request, wait for the reply.
	call("Master.GetTask", &args, &reply)

	if (reply.HasTask) {
		// reply.Y should be 100.
		fmt.Printf("reply.TaskInfo.WorkerId %v\n", reply.TaskInfo.WorkerId)
		return reply.TaskInfo, reply.NReduce
	}
	return Task{}, 0
}

func notifyTaskCompleted(wrokerId string, taskNumber int) {
	// declare an argument structure.
	args := WorkerCompletedRequest{}

	// fill in the argument(s).
	args.WorkerId = wrokerId
	args.TaskNumber = taskNumber

	// declare a reply structure.
	reply := WorkerCompletedResponse{}

	// send the RPC request, wait for the reply.
	call("Master.WorkerCompleted", &args, &reply)

	if (reply.Success) {
		// reply.Y should be 100.
		fmt.Printf("reply.Success %v\n", reply.Success)
	}
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

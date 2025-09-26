package mr

import "fmt"
import "log"
import "net"
import "net/rpc"
import "net/http" // HTTP 服务
import "hash/fnv"
import "sort"
import "os"
import "io/ioutil"
import "encoding/json"
import "time"


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

type WorkerNode struct {
	masterClient *rpc.Client // master服务器的client 像master发送RPC请求
	workerId string //
	task chan Task // 等待执行的任务
	NReduce int
}

// server 启动 RPC 服务器
// 启动一个监听 Worker RPC 调用的线程
func (worker *WorkerNode) server() {
	rpc.Register(worker)       // 注册 Worker 为 RPC 服务
	rpc.HandleHTTP()      // 设置 HTTP 处理器
	sockname := workerSock(worker.workerId) // 获取 socket 名称
	os.Remove(sockname)   // 删除可能存在的旧 socket 文件

	// 创建 Unix domain socket 监听器
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e) // 监听失败则退出程序
	}

	// 在新的 goroutine 中启动 HTTP 服务
	go http.Serve(l, nil)
}

func (worker *WorkerNode) ReceiveTask(args *AssignTaskRequest, reply *AssignTaskResponse) error {
	log.Printf("接受到任务:%v",args.TaskInfo)
	worker.task <- args.TaskInfo
	worker.NReduce = args.NReduce
	reply.Success = true
	return nil
}

func (worker *WorkerNode) Exit(args *WorkerExitRequest, reply *WorkerExitResponse) error {
	log.Printf("退出:")
	// 创建退出任务并发送到任务channel
	exitTask := Task{
		Number: -1, // 特殊编号表示退出任务
		Type:   ExitTask,
	}
	worker.task <- exitTask
	log.Printf("退出任务已发送")
	reply.Success = true
	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// 生成唯一的 Worker ID，包含主机名、端口和时间戳
	hostname, _ := os.Hostname()
	workerId := fmt.Sprintf("%s:%d-%d", hostname, 0, time.Now().UnixNano())
	// 创建worker节点
	worker := WorkerNode{
		masterClient: makeMasterClient(),
		workerId: workerId,
		task: make(chan Task, 2),
	}

	// 启动worker RPC服务器
	worker.server()

	// 注册worker
	worker.register()

	// 等待分配任务执行
	for {
		task := <- worker.task
		log.Printf("开始执行任务：%v", task)

		// 检查是否为退出任务
		if task.Type == ExitTask {
			log.Printf("收到退出任务，worker准备退出")
			return
		}

		// 拿到任务开始执行
		// 判断是Map任务还是Reduce任务
		taskType := MapTask
		if (task.Type == MapTask) {
			// 执行map任务
			execMap(task, mapf, worker.NReduce)
		} else if (task.Type == ReduceTask) {
			// 执行	Reduce任务
			taskType = ReduceTask
			execReduce(task, reducef)
		}
		// 执行完成 ，通知master
		worker.notifyTaskCompleted(workerId, task.Number, Idle, taskType)
	}
}

func readInterMediateData(filenames []string) []KeyValue {
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		// log.Printf("读取中间文件：%s", filename)
		intermediateFile, err := os.Open(filename)
		if err != nil {
			// log.Fatalf("无法打开中间文件 %s: %v", filename, err)
		}
		defer intermediateFile.Close()
		// 解码
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			break
			}
			intermediate = append(intermediate, kv)
		}
	}
	return intermediate
}

func execReduce(task Task, reducef func(string, []string) string) {
	// 读取中间数据
	intermediate := readInterMediateData(task.Filename)
	// fmt.Printf("Reduce 任务 %d 读取 %d 个中间键值对\n", task.Number, len(intermediate))

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
	content := readFile(task.Filename[0])

	// 调用用户实现的 Map 函数，返回一组 KeyValue
	mapRes := mapf(task.Filename[0], content)
	fmt.Printf("Map 任务 %d 处理文件 %s，生成 %d 个中间键值对\n", task.Number, task.Filename[0], len(mapRes))
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

func (worker WorkerNode) register() {

	// declare an argument structure.
	args := RegisterWorkerRequest{}

	// // fill in the argument(s).
	// args.Hostname,_ = os.Hostname()
	// args.Port = 0
	args.WorkerId = worker.workerId

	// declare a reply structure.
	reply := RegisterWorkerResponse{}

	// send the RPC request, wait for the reply.
	worker.call("Master.RegisterWorker", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.WorkerId %v\n", reply.WorkerId)
}

// func getTask(workerId string) (Task, int, int) {

// 	// declare an argument structure.
// 	args := GetTaskRequest{}

// 	// fill in the argument(s).
// 	args.WorkerId = workerId

// 	// declare a reply structure.
// 	reply := GetTaskResponse{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.GetTask", &args, &reply)

// 	if (reply.HasTask) {
// 		// reply.Y should be 100.
// 		// fmt.Printf("reply.TaskInfo.WorkerId %v\n", reply.TaskInfo.WorkerId)
// 		return reply.TaskInfo, reply.NReduce, reply.Status
// 	}
// 	return Task{}, reply.NReduce, reply.Status
// }

func (worker WorkerNode) notifyTaskCompleted(workerId string, taskNumber int, status WorkerStatus, taskType TaskType) {
	// declare an argument structure.
	args := WorkerCompletedRequest{}

	// fill in the argument(s).
	args.WorkerId = workerId
	args.TaskNumber = taskNumber
	args.Status = status
	args.TaskType = taskType

	// declare a reply structure.
	reply := WorkerCompletedResponse{}

	// send the RPC request, wait for the reply.
	worker.call("Master.WorkerCompleted", &args, &reply)

	if (reply.Success) {
		// reply.Y should be 100.
		fmt.Printf("reply.Success %v\n", reply.Success)
	}
}

func makeMasterClient() *rpc.Client {
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return c
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func (worker WorkerNode) call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := masterSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	// if err != nil {
	// 	log.Fatal("dialing:", err)
	// }
	// defer c.Close()


	err := worker.masterClient.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "io"
import "fmt"

type WorkerStatus struct {
	code	string
	desc	string
}

const idle = WorkerStatus{
	code: "idle",
	desc: "空闲"
}

const inProgress = WorkerStatus{
	code: "in-progress",
	desc: "任务进行中"
}

const completed = WorkerStatus{
	code: "completed",
	desc: "任务执行完成"
}

type Worker struct {
	Id 			string
	Hostname 	string
	Port 		int
	[]Tasks  Task
	Status		WorkerStatus
}

type TaskType struct {
	code string
	desc string
}

const mapTask = TaskType{
	code: "map",
	desc: "map task"
}

const reduceTask = TaskType{
	code: "reduce",
	desc: "reduce task"
}

type Task struct {
	Number int
	Filename string
	WorkerId string
	StartTime time.Time
	Status int
	Type TaskType
}

type Master struct {
	// Your definitions here.
	[]Workers Worker
	[]Tasks Task
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 注册worker
func (m *Master) RegisterWorker(args *RegisterWorkerRequest, reply *RegisterWorkerResponse) error {
	// 生成唯一 Worker ID
    workerId := fmt.Sprintf("%s:%d-%d", args.Hostname, args.Port, time.Now().UnixNano())
	worker := Worker{
		Id: workerId,
		Hostname: args.Hostname,
		Port: args.Port,
		Status: idle
	}
	reply.WorkerId = workerId
	m.Workers = append(m.Workers, worker)
	return nil;
}

// worker获取任务
func (m *Master) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	// 找到这个worker能执行的任务
	// todo by 文件 维度
	// 循环找到状态为0的Task
	for _, task range m.tasks {
		if (task.Status == 0) {
			// 修改这个任务的状态
			task.Status = 1
			task.WorkerId = args.WrokerId
			task.StartTime = Time.Now()
			// 将这个任务返回给Worker
			reply.TaskInfo = task
			return nil
		}
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// 读取每个输入文件，调用 Map，将所有中间结果收集到一个切片中。
	// intermediate := [][]string{}
	taskList := []Task
	taskNumber := 0
	for _, filename := range files {
		task := Task{
			Number: taskNumber++,
			Filename: filename,
			Status: 0,
			Type: mapTask
		}
		taskList = append(taskList, task)

		// intermediate = append(intermediate, split(filename))
		// 将数据分片 分成多个chunk，等待worker线程获取
		// kva := mapf(filename, string(content))
		// // 合并到总体的 intermediate 列表
		// intermediate = append(intermediate, kva...)
	}

	// 创建reduce任务
	for (i := 0; i < nReduce; i++) {
		task := Task{
			Number: taskNumber++,
			// todo 替换成content
			// Filename: filename,
			Status: 0,
			Type: reduceTask
		}
		taskList = append(taskList, task)
	}
	m.Tasks = taskList
	m.nReduce = nReduce

	m.server()
	fmt.Println("map reduce master server");
	return &m
}

func split(filename string) []string {
	res := []string{}
	// 读取文件
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 定义块大小（16KB）
	const chunkSize = 64 * 1024 * 1024 // 64MB
	// 创建缓冲区
	buffer := make([]byte, chunkSize)
	for {
		// 读取数据到缓冲区
		bytesRead, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("cannot read bytes for file %v", filename)
		}

		// 创建块副本并添加到列表
		chunk := make([]byte, bytesRead)
		// 将字节转换为字符串并添加到结果切片
		res = append(res, string(chunk[:bytesRead]))
		// 如果已到达文件末尾，退出循环
		if err == io.EOF {
			break
		}
	}
	// 输出结果
	fmt.Printf("共读取 %d 个块\n", len(res))
	for i, chunk := range res {
		fmt.Printf("块 #%d: %d 字符\n", i+1, len(chunk))
		// 如果需要查看内容，可以取消下面的注释
		// fmt.Printf("内容: %s\n", chunk)
	}
	return res
	// 读取16KB的内容到内存中
	// content, err := ioutil.ReadAll(file)
	// if err != nil {
	// 	log.Fatalf("cannot read %v", filename)
	// }
}

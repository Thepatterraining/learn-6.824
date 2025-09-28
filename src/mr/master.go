package mr

import (
	"fmt"      // 格式化输出
	"io"       // IO 操作
	"log"      // 日志记录
	"net"      // 网络操作
	"net/http" // HTTP 服务
	"net/rpc"  // RPC 服务
	"os"       // 操作系统接口
	"sync"     // 同步原语
	"time"     // 时间操作
)

// WorkerStatus 定义 Worker 的状态结构
type WorkerStatus struct {
	Code string // 状态代码
	Desc string // 状态描述
}

// 定义 Worker 状态常量
var (
	// Idle Worker 空闲状态
	Idle = WorkerStatus{
		Code: "idle",
		Desc: "空闲",
	}
	// inProgress Worker 任务进行中状态
	inProgress = WorkerStatus{
		Code: "in-progress",
		Desc: "任务进行中",
	}
	// completed Worker 任务完成状态
	Completed = WorkerStatus{
		Code: "completed",
		Desc: "任务执行完成",
	}
)

// Worker 定义工作节点结构
type WorkerStruct struct {
	Id       string       // Worker 唯一标识
	Hostname string       // 主机名
	Port     int          // 端口号
	Tasks    []Task       // 分配给该 Worker 的任务列表
	Status   WorkerStatus // Worker 当前状态
	RpcClient *rpc.Client
}

// TaskType 定义任务类型结构
type TaskType struct {
	Code string // 任务类型代码
	Desc string // 任务类型描述
}

// 定义任务类型常量
var (
	// MapTask Map 任务类型
	MapTask = TaskType{
		Code: "map",
		Desc: "map task",
	}
	// ReduceTask Reduce 任务类型
	ReduceTask = TaskType{
		Code: "reduce",
		Desc: "reduce task",
	}
	// ExitTask 退出任务类型
	ExitTask = TaskType{
		Code: "exit",
		Desc: "exit task",
	}
)

// TaskStatus 定义任务状态常量
const (
	TaskStatusPending    = 0 // 任务待执行
	TaskStatusInProgress = 1 // 任务执行中
	TaskStatusCompleted  = 2 // 任务已完成
	TaskStatusFailed     = 3 // 任务执行失败
)

// Task 定义任务结构
type Task struct {
	Number    int       // 任务编号
	Filename  []string    // 文件名（对于 Map 任务）
	WorkerId  string    // 分配给的 Worker ID
	StartTime time.Time // 任务开始时间
	EndTime   time.Time // 任务结束时间
	Status    int       // 任务状态
	Type      TaskType  // 任务类型
}

// Master 定义主节点结构
type Master struct {
	IdleWorkers chan WorkerStruct
	PenddingTasks chan Task // 待执行的task
	nReduce int      // Reduce 任务数量
	mu      sync.Mutex // 互斥锁，保证线程安全
	completedTaskCount int // 完成任务数量
	mapTaskCount int // Map任务总数
	completedMapTaskCount int // 完成的Map任务数量
	reduceTasks []*Task // reduce 任务
	WorkerMap map[string]*WorkerStruct
	TaskMap map[int]*Task
	isDone chan bool
	healthDone chan bool
	listener net.Listener
}

// server 启动 RPC 服务器
// 启动一个监听 Worker RPC 调用的线程
func (m *Master) server() {
	rpc.Register(m)       // 注册 Master 为 RPC 服务
	rpc.HandleHTTP()      // 设置 HTTP 处理器
	sockname := masterSock() // 获取 socket 名称
	os.Remove(sockname)   // 删除可能存在的旧 socket 文件

	// 创建 Unix domain socket 监听器
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e) // 监听失败则退出程序
	}

	m.listener = l
	// 在新的 goroutine 中启动 HTTP 服务
	go http.Serve(l, nil)
}

func makeWorkerClient(workerId string) *rpc.Client {
	sockname := workerSock(workerId)
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return c
}

// RegisterWorker 注册新的 Worker
// 当 Worker 启动时调用此方法向 Master 注册
func (m *Master) RegisterWorker(args *RegisterWorkerRequest, reply *RegisterWorkerResponse) error {
	// 创建新的 Worker 实例
	worker := WorkerStruct{
		Id:       args.WorkerId,
		Hostname: args.Hostname,
		Port:     args.Port,
		Tasks:    make([]Task, 0), // 初始化空任务列表
		Status:   Idle,            // 初始状态为空闲
		RpcClient: makeWorkerClient(args.WorkerId), // 客户端
	}

	// reply.WorkerId = workerId                        // 返回生成的 Worker ID
	m.mu.Lock()         // 加锁保证线程安全
	defer m.mu.Unlock() // 函数结束时解锁
	// m.Workers = append(m.Workers, worker)            // 将新 Worker 添加到列表
	m.IdleWorkers <- worker // 空闲worker
	m.WorkerMap[args.WorkerId] = &worker
	log.Printf("注册新 Worker: %s", args.WorkerId)        // 记录日志
	return nil
}

// 通知 Master， Worker 任务完成
func (m *Master) WorkerCompleted(args *WorkerCompletedRequest, reply *WorkerCompletedResponse) error {
	m.mu.Lock()         // 加锁保证线程安全
	defer m.mu.Unlock() // 函数结束时解锁

	// 更新完成任务数量
	m.completedTaskCount++

	// 更新任务状态
	task := m.TaskMap[args.TaskNumber]
	task.Status = TaskStatusCompleted
	task.EndTime = time.Now()

	// 更新worker状态
	worker := m.WorkerMap[args.WorkerId]
	worker.Status = Idle
	worker.Tasks = make([]Task, 0)
	m.IdleWorkers <- *worker
	log.Printf("worker:%v 完成了任务:%v", *worker, *task)
	// 如果完成的是 map 任务，更新对应的 reduce 任务
	if (task.Type == MapTask) {
		m.completedMapTaskCount++
		for i := range m.reduceTasks {
			reduceTask := m.reduceTasks[i]
			// 生成对应的文件名并更新
			filename := fmt.Sprintf("mr-%d-%d", args.TaskNumber, i)
			// log.Printf("生成文件名:%s", filename)
			reduceTask.Filename = append(reduceTask.Filename, filename)
			m.reduceTasks[i] = reduceTask // 重要：更新slice中的任务
		}

		// 检查所有Map任务是否完成
		if (m.completedMapTaskCount == m.mapTaskCount) {
			log.Printf("所有Map任务完成，开始分配Reduce任务")
			// 可以分配reduce任务
			for _, reduceTask:= range m.reduceTasks {
				log.Printf("reduce task:%v", reduceTask)
				m.PenddingTasks <- *reduceTask
			}
		}
	}
	reply.Success = true
	m.checkCompleted()
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.completedTaskCount == len(m.TaskMap);
}

func (m *Master) checkCompleted() {
	if m.completedTaskCount == len(m.TaskMap) {
		m.isDone <- true
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// files: 输入文件列表
// nReduce: Reduce 任务数量
func MakeMaster(files []string, nReduce int) *Master {
	// 创建 Master 实例
	m := Master{
		nReduce: nReduce,           // 设置 Reduce 任务数量
		IdleWorkers: make(chan WorkerStruct, 100), // 空闲Worker
		PenddingTasks: make(chan Task, 100), //待执行的任务
		reduceTasks: make([]*Task, nReduce), //reduce task
		completedTaskCount: 0,
		mapTaskCount: len(files), // Map任务总数等于输入文件数
		completedMapTaskCount: 0,
		WorkerMap: make(map[string]*WorkerStruct, 100),
		TaskMap: make(map[int]*Task, 100),
		isDone: make(chan bool, 0),
		healthDone: make(chan bool, 0),
	}

	// 为每个输入文件创建 Map 任务
	maxTaskNumber := 0
	for _, filename := range files {
		task := Task{
			Number:   maxTaskNumber,        // 分配任务编号
			Filename: []string{filename},          // 设置文件名
			Status:   TaskStatusPending, // 初始状态为待执行
			Type:     MapTask,           // 设置为 Map 任务类型
		}
		m.PenddingTasks <- task // 添加到任务列表
		m.TaskMap[maxTaskNumber] = &task
		maxTaskNumber++                    // 递增任务编号
		// log.Printf("创建 Map 任务 %d: %s", task.Number, filename)
	}

	// 创建 Reduce 任务
	for i := 0; i < nReduce; i++ {
		task := Task{
			Filename: make([]string, 0),
			Number: maxTaskNumber,        // 分配任务编号
			Status: TaskStatusPending, // 初始状态为待执行
			Type:   ReduceTask,        // 设置为 Reduce 任务类型
		}
		m.reduceTasks[i] = &task // 添加到任务列表
		m.TaskMap[maxTaskNumber] = &task
		maxTaskNumber++                    // 递增任务编号
		// log.Printf("创建 Reduce 任务 %v", task)
	}

	// 启动 RPC 服务器
	m.server()

	// 分配任务
	go m.taskSchdule()
	// log.Printf("MapReduce Master 服务器启动，共 %d 个任务", len(m.Tasks))

	// 健康检查
	go m.health()

	return &m // 返回 Master 实例指针
}

func (m *Master) health() {
	// 每10s ping一次 worker
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			for i, worker := range m.WorkerMap {
				log.Printf("worker健康检查： %v", worker)
				// declare an argument structure.
				args := WorkerHealthRequest{}

				// declare a reply structure.
				reply := WorkerHealthResponse{}
				if (!worker.call("WorkerNode.Health", &args, &reply)) {
					// 将worker执行的任务重新放入待执行队列
					for _, task := range worker.Tasks {
						// 重置任务状态
						taskInMap, exists := m.TaskMap[task.Number]
						if exists {
							taskInMap.Status = TaskStatusPending
							taskInMap.WorkerId = ""
							// m.TaskMap[task.Number] = taskInMap
						}
						// 重新加入待执行队列
						m.PenddingTasks <- task
					}

					// 清空worker的任务列表
					worker.Tasks = make([]Task, 0)
					// 关闭worker链接
					worker.RpcClient.Close()
					delete(m.WorkerMap, i)
					// m.WorkerMap[workerId] = worker // 更新map中的worker
				}
			}
			m.mu.Unlock()
		case <-m.healthDone:
			// 收到完成信号，退出健康检查
			log.Printf("健康检查器退出")
			return
		}
	}
}


/**
* 分配任务
*/
func (m *Master) taskSchdule() {
	// 找到空闲的worker
	for {
		log.Printf("调度器开始调度")
		select {
		case task := <- m.PenddingTasks:
			log.Printf("空闲任务:%v", task)
			worker := <- m.IdleWorkers
			log.Printf("空闲worker: %v", worker)
			// 更新任务状态
			temptask := m.TaskMap[task.Number]
			temptask.Status = TaskStatusInProgress
			temptask.StartTime = time.Now()
			temptask.WorkerId = worker.Id
			// 更新worker
			tempWorker := m.WorkerMap[worker.Id]
			tempWorker.Tasks = append(worker.Tasks, *temptask)
			tempWorker.Status = inProgress
			// 任务分配给worker
			go m.assignTask(task, worker)
		case <- m.isDone:
			// 退出
			log.Printf("所有任务已完成")
			m.Exit()
			return
		}
	}
}

func(m *Master) Exit() {
	log.Printf("退出master")
	// 通知所有worker退出
	m.notifyWorkerExit()
	// 2. 停止健康检查协程
    m.healthDone <- true

	// 3. 关闭RPC服务器
	if m.listener != nil {
		m.listener.Close()
		// log.Printf("RPC服务器已关闭")
	}

	// 4. 清理socket文件
	sockname := masterSock()
	os.Remove(sockname)
	// log.Printf("Socket文件已清理")

	// 5. 关闭所有worker RPC连接
	for _, worker := range m.WorkerMap {
		if worker.RpcClient != nil {
			worker.RpcClient.Close()
		}
	}

	// log.Printf("所有清理操作完成")
}

func (m *Master) assignTask(task Task, worker WorkerStruct) {
	// declare an argument structure.
	args := AssignTaskRequest{}

	// fill in the argument(s).
	args.TaskInfo = task
	args.NReduce = m.nReduce

	// declare a reply structure.
	reply := AssignTaskResponse{}
	worker.call("WorkerNode.ReceiveTask", &args, &reply)
}

func (m *Master) notifyWorkerExit() {
	for _, worker := range m.WorkerMap {
		log.Printf("通知worker退出： %v", worker)
		// declare an argument structure.
		args := WorkerExitRequest{}

		// declare a reply structure.
		reply := WorkerExitResponse{}
		worker.call("WorkerNode.Exit", &args, &reply)
	}
}

// split 将文件分割成多个块
// 此函数用于将大文件分割成适合处理的小块
// filename: 要分割的文件名
// 返回: 文件内容块的字符串切片
func split(filename string) []string {
	res := []string{} // 结果切片

	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("无法打开文件 %v: %v", filename, err)
	}
	defer file.Close() // 确保文件在函数结束时关闭

	// 定义块大小为 64MB
	const chunkSize = 64 * 1024 * 1024
	buffer := make([]byte, chunkSize) // 创建缓冲区

	// 循环读取文件内容
	for {
		// 读取数据到缓冲区
		bytesRead, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				// 到达文件末尾，正常退出
				break
			}
			log.Fatalf("读取文件 %v 时出错: %v", filename, err)
		}

		// 如果读取到数据，添加到结果中
		if bytesRead > 0 {
			// 创建块副本并转换为字符串
			chunk := string(buffer[:bytesRead])
			res = append(res, chunk)
		}

		// 如果读取的字节数小于缓冲区大小，说明已到文件末尾
		if bytesRead < chunkSize {
			break
		}
	}

	// 输出分割结果统计
	log.Printf("文件 %s 共分割为 %d 个块", filename, len(res))
	for i, chunk := range res {
		log.Printf("块 #%d: %d 字符", i+1, len(chunk))
	}

	return res
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func (workerStruct WorkerStruct) call(rpcname string, args interface{}, reply interface{}) bool {
	err := workerStruct.RpcClient.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

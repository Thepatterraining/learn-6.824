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
	// idle Worker 空闲状态
	idle = WorkerStatus{
		Code: "idle",
		Desc: "空闲",
	}
	// inProgress Worker 任务进行中状态
	inProgress = WorkerStatus{
		Code: "in-progress",
		Desc: "任务进行中",
	}
	// completed Worker 任务完成状态
	completed = WorkerStatus{
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
	Filename  string    // 文件名（对于 Map 任务）
	WorkerId  string    // 分配给的 Worker ID
	StartTime time.Time // 任务开始时间
	EndTime   time.Time // 任务结束时间
	Status    int       // 任务状态
	Type      TaskType  // 任务类型
}

// Master 定义主节点结构
type Master struct {
	Workers []WorkerStruct // Worker 列表
	Tasks   []Task   // 任务列表
	nReduce int      // Reduce 任务数量
	MaxTaskNumber int 	// 最大任务编号
	mu      sync.Mutex // 互斥锁，保证线程安全
	// done    bool     // 标记所有任务是否完成
}

// Example RPC 处理器示例
// 这是一个示例 RPC 处理器，展示如何定义 RPC 方法
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1 // 简单的加1操作
	return nil
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

	// 在新的 goroutine 中启动 HTTP 服务
	go http.Serve(l, nil)
}

// RegisterWorker 注册新的 Worker
// 当 Worker 启动时调用此方法向 Master 注册
func (m *Master) RegisterWorker(args *RegisterWorkerRequest, reply *RegisterWorkerResponse) error {
	// m.mu.Lock()         // 加锁保证线程安全
	// defer m.mu.Unlock() // 函数结束时解锁

	// 生成唯一的 Worker ID，包含主机名、端口和时间戳
	workerId := fmt.Sprintf("%s:%d-%d", args.Hostname, args.Port, time.Now().UnixNano())

	// 创建新的 Worker 实例
	worker := WorkerStruct{
		Id:       workerId,
		Hostname: args.Hostname,
		Port:     args.Port,
		Tasks:    make([]Task, 0), // 初始化空任务列表
		Status:   idle,            // 初始状态为空闲
	}

	reply.WorkerId = workerId                        // 返回生成的 Worker ID
	m.Workers = append(m.Workers, worker)            // 将新 Worker 添加到列表
	log.Printf("注册新 Worker: %s", workerId)        // 记录日志
	return nil
}

// GetTask Worker 获取任务
// Worker 调用此方法从 Master 获取待执行的任务
func (m *Master) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	// m.mu.Lock()         // 加锁保证线程安全
	// defer m.mu.Unlock() // 函数结束时解锁

	// 遍历任务列表，查找待执行的任务
	for i := range m.Tasks {
		task := &m.Tasks[i] // 获取任务指针以便修改

		// 检查任务是否为待执行状态
		if task.Status == TaskStatusPending {
			// 更新任务状态为执行中
			task.Status = TaskStatusInProgress
			task.WorkerId = args.WorkerId // 分配给请求的 Worker
			task.StartTime = time.Now()   // 记录开始时间

			// 将任务信息返回给 Worker
			reply.TaskInfo = *task
			reply.HasTask = true // 标记有可用任务
			reply.NReduce = m.nReduce

			log.Printf("分配任务 %d 给 Worker %s", task.Number, args.WorkerId)
			return nil
		}
	}

	// 没有找到可用任务
	reply.HasTask = false
	log.Printf("没有可用任务分配给 Worker %s", args.WorkerId)
	return nil
}

// 通知 Master， Worker 任务完成
func (m *Master) WorkerCompleted(args *WorkerCompletedRequest, reply *WorkerCompletedResponse) error {
	// m.mu.Lock()         // 加锁保证线程安全
	// defer m.mu.Unlock() // 函数结束时解锁

	// 查找对应 Worker 并更新状态
	for i := range m.Workers {
		worker := &m.Workers[i]
		if (worker.Id == args.WorkerId) {
			// 修改worker的状态
			worker.Status = idle
		}
	}

	// 查找对应的task 并更新状态
	for i := range m.Tasks {
		task := &m.Tasks[i]
		if (task.Number == args.TaskNumber) {
			// 更新状态
			task.Status = TaskStatusCompleted
			task.EndTime = time.Now()
			// 如果完成的是 map 任务，创建对应的 reduce 任务
			if (task.Type == MapTask) {
				for i := 0; i < m.nReduce; i++ {
					filename := fmt.Sprintf("mr-%d-%d", task.Number, i)
					task := Task{
						Number: m.MaxTaskNumber,        // 分配任务编号
						Status: TaskStatusPending, // 初始状态为待执行
						Type:   ReduceTask,        // 设置为 Reduce 任务类型
						Filename: filename, 			// Reduce 任务文件名
					}
					m.Tasks = append(m.Tasks, task) // 添加到任务列表
					m.MaxTaskNumber++                    // 递增任务编号
					log.Printf("创建 Reduce 任务 %d", task.Number)
				}
			}
		}
	}
	reply.Success = true
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	return ret;
}

// TaskCompleted 标记任务完成
// Worker 完成任务后调用此方法通知 Master
// func (m *Master) TaskCompleted(args *TaskCompletedRequest, reply *TaskCompletedResponse) error {
// 	// m.mu.Lock()         // 加锁保证线程安全
// 	// defer m.mu.Unlock() // 函数结束时解锁

// 	// 查找对应的任务并更新状态
// 	for i := range m.Tasks {
// 		task := &m.Tasks[i]
// 		if task.Number == args.TaskNumber && task.WorkerId == args.WorkerId {
// 			task.Status = TaskStatusCompleted // 标记任务为已完成
// 			log.Printf("任务 %d 已完成，Worker: %s", task.Number, args.WorkerId)

// 			// 检查是否所有任务都已完成
// 			m.checkAllTasksCompleted()
// 			return nil
// 		}
// 	}

// 	log.Printf("未找到任务 %d，Worker: %s", args.TaskNumber, args.WorkerId)
// 	return fmt.Errorf("task not found")
// }

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
		Workers: make([]WorkerStruct, 0), // 初始化空 Worker 列表
		Tasks:   make([]Task, 0),   // 初始化空任务列表
		nReduce: nReduce,           // 设置 Reduce 任务数量
		MaxTaskNumber: 0,			// 初始化最大任务编号
		// done:    false,             // 初始状态为未完成
	}

	// 为每个输入文件创建 Map 任务
	for _, filename := range files {
		task := Task{
			Number:   m.MaxTaskNumber,        // 分配任务编号
			Filename: filename,          // 设置文件名
			Status:   TaskStatusPending, // 初始状态为待执行
			Type:     MapTask,           // 设置为 Map 任务类型
		}
		m.Tasks = append(m.Tasks, task) // 添加到任务列表
		m.MaxTaskNumber++                    // 递增任务编号

		log.Printf("创建 Map 任务 %d: %s", task.Number, filename)
	}

	// 创建 Reduce 任务
	// for i := 0; i < nReduce; i++ {
	// 	task := Task{
	// 		Number: taskNumber,        // 分配任务编号
	// 		Status: TaskStatusPending, // 初始状态为待执行
	// 		Type:   ReduceTask,        // 设置为 Reduce 任务类型
	// 	}
	// 	m.Tasks = append(m.Tasks, task) // 添加到任务列表
	// 	taskNumber++                    // 递增任务编号

	// 	log.Printf("创建 Reduce 任务 %d", task.Number)
	// }

	// 启动 RPC 服务器
	m.server()
	log.Printf("MapReduce Master 服务器启动，共 %d 个任务", len(m.Tasks))

	return &m // 返回 Master 实例指针
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

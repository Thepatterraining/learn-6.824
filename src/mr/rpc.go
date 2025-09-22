package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// RegisterWorkerRequest 注册 Worker 请求结构
type RegisterWorkerRequest struct {
	Hostname string // Worker 主机名
	Port     int    // Worker 端口号
}

// RegisterWorkerResponse 注册 Worker 响应结构
type RegisterWorkerResponse struct {
	WorkerId string // 分配给 Worker 的唯一 ID
}

// GetTaskRequest Worker 获取任务请求结构
type GetTaskRequest struct {
	WorkerId string // 请求任务的 Worker ID
}

// GetTaskResponse Worker 获取任务响应结构
type GetTaskResponse struct {
	HasTask  bool // 是否有可用任务
	TaskInfo Task // 任务详细信息
	NReduce  int  // 任务对应的 Reduce 任务数量
}

// TaskCompletedRequest 任务完成通知请求结构
type TaskCompletedRequest struct {
	TaskNumber int    // 完成的任务编号
	WorkerId   string // 完成任务的 Worker ID
}

// TaskCompletedResponse 任务完成通知响应结构
type TaskCompletedResponse struct {
	Success bool // 是否成功处理完成通知
}

type WorkerCompletedRequest struct {
	WorkerId string
	TaskNumber int
}

type WorkerCompletedResponse struct {
	Success bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

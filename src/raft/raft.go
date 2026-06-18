package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"learn-6.824/src/labgob"
	"learn-6.824/src/labrpc"
)

type ShardKVStatus string

const (
	Normal       ShardKVStatus = "NORMAL"        // 属于本组的分片，正常读写
	MigrationOut ShardKVStatus = "MIGRATION_OUT" // 之前属于我，现在要迁移出去的分片，禁止读写
	MigrationIn  ShardKVStatus = "MIGRATION_IN"  // 之前不属于，现在要迁移进来的分片，禁止读写
	Invalid      ShardKVStatus = "INVALID"       // 不属于本组的分片
)

// TraceContext 用于追踪请求链路
type TraceContext struct {
	TraceID string
	From    int
	To      int
}

// EventType 事件类型
type EventType string

const (
	RPC_RECV       EventType = "RPC_RECV"
	RPC_SEND       EventType = "RPC_SEND"
	STATE_CHANGE   EventType = "STATE_CHANGE"
	LOG_COMMIT     EventType = "LOG_COMMIT"
	LOG_REPLICA    EventType = "LOG_REPLICA"
	TIMER_EVENT    EventType = "TIMER_EVENT"
	ELECTION_EVENT EventType = "ELECTION_EVENT"
	HEARTBEAT      EventType = "HEARTBEAT"
)

// Logger 结构化日志记录器
type Logger struct {
	nodeId int
}

// NewLogger 创建新的日志记录器
func NewLogger(nodeId int) *Logger {
	return &Logger{nodeId: nodeId}
}

// LogWithTrace 记录带追踪信息的日志
func (l *Logger) LogWithTrace(eventType EventType, trace TraceContext, format string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")

	message := fmt.Sprintf(format, args...)

	traceInfo := ""
	if trace.TraceID != "" {
		if trace.From >= 0 && trace.To >= 0 {
			traceInfo = fmt.Sprintf("[Trace:%s][%d->%d]", trace.TraceID, trace.From, trace.To)
		} else {
			traceInfo = fmt.Sprintf("[Trace:%s]", trace.TraceID)
		}
	}

	DPrintf("[%s] [Node:%d] [%s] %s %s",
		timestamp, l.nodeId, eventType, traceInfo, message)
}

// LogStateChange 记录状态变化
func (l *Logger) LogStateChange(from, to RaftStatus, term int, reason string) {
	timestamp := time.Now().Format("15:04:05.000")

	DPrintf("[%s] [Node:%d] [%s] [STATE_CHANGE] %s->%s term:%d reason:%s",
		timestamp, l.nodeId, "INFO",
		getStatusString(from), getStatusString(to), term, reason)
}

// getStatusString 获取状态字符串
func getStatusString(status RaftStatus) string {
	switch status {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// generateTraceID 生成追踪ID
func generateTraceID() string {
	return fmt.Sprintf("%d%d", time.Now().UnixNano(), rand.Intn(1000))
}

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// import "bytes"
// import "../labgob"

// RaftStatus represents the state of a Raft node
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid  bool
	Command       interface{}
	CommandIndex  int
	Snapshot      map[string]string
	IsSnapshot    bool
	SeqNums       map[int64]int64 // 快照包含的seqNums，用于幂等性检测
	Shard2Data    map[int]map[string]string
	Shard2SeqNums map[int]map[int64]int64
	Shard2Status  map[int]ShardKVStatus
	ConfigNum     int
}

type RaftStatus int

const (
	Follower RaftStatus = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{}
	Term    int
	Commit  bool
	Index   int
}

// 快照
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              map[string]string
	SeqNums           map[int64]int64
	Shard2Data        map[int]map[string]string
	Shard2SeqNums     map[int]map[int64]int64
	Shard2Status      map[int]ShardKVStatus
	ConfigNum         int
}

// A Go object implementing a single Raft peer.
// 实现单个Raft对等体的Go对象
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	logger *Logger // 结构化日志记录器

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 需要持久化
	currentTerm int        //当前任期
	votedFor    int        // 投票给谁
	log         []LogEntry // log信息

	// 不需要持久化
	commitIndex int // 已提交的最大索引
	lastApplied int // 已应用到状态机的最大索引

	// leader的属性
	nextIndex  []int // 对每个 follower，下次要发送给每个follower的日志索引
	matchIndex []int // 对每个 follower，对端已复制的最大日志索引

	// 节点状态
	status RaftStatus
	// 心跳时间
	lastHeartBeatTime atomic.Value
	// 条件变量用于心跳控制
	heartbeatCond *sync.Cond
	// 标记是否需要立即发送心跳
	needHeartbeat bool
	// 标记心跳goroutine是否应该停止
	stopHeartbeat bool
	// 通道
	applyCh chan ApplyMsg
	// 最新的快照
	lastSnapshot Snapshot
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

func (rf *Raft) GetApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastApplied < rf.lastSnapshot.LastIncludedIndex {
		return rf.lastSnapshot.LastIncludedIndex
	}
	return rf.lastApplied
}

func (rf *Raft) CreateShardSnapshot2(snapshot Snapshot, lastIncludedIndex int) {
	rf.mu.Lock()
	snapshot.LastIncludedIndex = lastIncludedIndex
	snapshot.LastIncludedTerm = rf.log[rf.getLogStartIndex(lastIncludedIndex)].Term
	DPrintf("[Node:%d] CreateSnapshot snapshot 上一个index:%d snapshot:%v 开始", rf.me, rf.lastSnapshot.LastIncludedIndex, snapshot)
	// if rf.lastSnapshot.LastIncludedIndex >= lastIncludedIndex {
	// 	DPrintf("[Node:%d] CreateSnapshot snapshot 重复:%v 成功", rf.me, snapshot)
	// 	rf.mu.Unlock()
	// 	return
	// }
	rf.mu.Unlock()
	rf.saveSnapshot(snapshot, false, 0)

	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Snapshot: snapshot,
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	// 像所有Follower同步快照
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(i, &args, &reply)
		if ok {
			rf.mu.Lock()
			DPrintf("[Node:%d] 发送快照到 Node:%d 成功", rf.me, i)
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) CreateShardSnapshot(kvData map[int]map[string]string, lastIncludedIndex int, lastIncludedTerm int, seqNums map[int]map[int64]int64, shard2Status map[int]ShardKVStatus) {
	rf.mu.Lock()
	snapshot := Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  rf.log[rf.getLogStartIndex(lastIncludedIndex)].Term,
		Shard2Data:        kvData,
		Shard2SeqNums:     seqNums,
		Shard2Status:      shard2Status,
	}
	DPrintf("[Node:%d] CreateSnapshot snapshot 上一个index:%d snapshot:%v 开始", rf.me, rf.lastSnapshot.LastIncludedIndex, snapshot)
	// if rf.lastSnapshot.LastIncludedIndex >= lastIncludedIndex {
	// 	DPrintf("[Node:%d] CreateSnapshot snapshot 重复:%v 成功", rf.me, snapshot)
	// 	rf.mu.Unlock()
	// 	return
	// }
	rf.mu.Unlock()
	rf.saveSnapshot(snapshot, false, 0)

	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Snapshot: snapshot,
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	// 像所有Follower同步快照
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(i, &args, &reply)
		if ok {
			rf.mu.Lock()
			DPrintf("[Node:%d] 发送快照到 Node:%d 成功", rf.me, i)
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) CreateSnapshot(kvData map[string]string, lastIncludedIndex int, lastIncludedTerm int, seqNums map[int64]int64) {
	rf.mu.Lock()
	snapshot := Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  rf.log[rf.getLogStartIndex(lastIncludedIndex)].Term,
		Data:              kvData,
		SeqNums:           seqNums,
	}
	DPrintf("[Node:%d] CreateSnapshot snapshot 上一个index:%d snapshot:%v 开始", rf.me, rf.lastSnapshot.LastIncludedIndex, snapshot)
	// if rf.lastSnapshot.LastIncludedIndex >= lastIncludedIndex {
	// 	DPrintf("[Node:%d] CreateSnapshot snapshot 重复:%v 成功", rf.me, snapshot)
	// 	rf.mu.Unlock()
	// 	return
	// }
	rf.mu.Unlock()
	rf.saveSnapshot(snapshot, false, 0)

	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Snapshot: snapshot,
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	// 像所有Follower同步快照
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(i, &args, &reply)
		if ok {
			rf.mu.Lock()
			DPrintf("[Node:%d] 发送快照到 Node:%d 成功", rf.me, i)
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) saveSnapshot(snapshot Snapshot, discardAll bool, lastSnapshotIndex int) {
	// 丢弃早期的日志
	rf.mu.Lock()
	DPrintf("[Node:%d] saveSnapshot snapshot lastApplied:%d lastSnapshotIndex:%d 上一个index:%d snapshot:%v log :%v ", rf.me, snapshot.LastIncludedIndex, lastSnapshotIndex, rf.lastSnapshot.LastIncludedIndex, snapshot, rf.log)
	newLog := make([]LogEntry, 0)
	if len(rf.log) > 1 {
		// lastLogIndex := rf.getLastLogIndex()
		// index := 1
		// for i := snapshot.LastIncludedIndex + 1; i <= lastLogIndex; i++ {
		// 	logEntry := LogEntry{
		// 		Command: rf.log[i].Command,
		// 		Term:    rf.log[i].Term,
		// 		Commit:  false,
		// 		Index:   index,
		// 	}
		// 	newLog = append(newLog, logEntry)
		// 	index++
		// }
		lastLogIndex := rf.getLastLogIndex()
		startIndex := min(lastLogIndex, snapshot.LastIncludedIndex-rf.lastSnapshot.LastIncludedIndex)
		DPrintf("[Node:%d] saveSnapshot snapshot startIndex:%d lastLogIndex: %d", rf.me, startIndex, lastLogIndex)
		// if startIndex <= len(rf.log) {
		newLog = append(newLog, NewLogEntry(rf.log[0].Command, snapshot.LastIncludedTerm, 0))
		newLog = append(newLog, rf.log[startIndex+1:]...)
		// }
		// newLog = append(newLog, rf.log[snapshot.LastIncludedIndex+1:]...)
	} else {
		newLog = append(newLog, NewLogEntry(rf.log[0].Command, snapshot.LastIncludedTerm, 0))
	}
	rf.log = newLog
	rf.lastSnapshot = snapshot
	// 直接设置为快照的绝对索引，或者传入的lastSnapshotIndex参数
	// rf.commitIndex = 0
	rf.lastApplied = max(snapshot.LastIncludedIndex, rf.lastApplied)
	rf.commitIndex = rf.lastApplied
	DPrintf("[Node:%d] saveSnapshot rf.lastApplied:%d 丢弃后log :%v ", rf.me, rf.lastApplied, rf.log)
	// 修改 nextIndex 和 matchIndex
	// for i := range rf.peers {
	// 	rf.nextIndex[i] = rf.nextIndex[i] - snapshot.LastIncludedIndex
	// }
	// for i := range rf.peers {
	// 	rf.nextIndex[i] = rf.getLastLogIndex() + 1
	// 	matchIndex := rf.matchIndex[i] - snapshot.LastIncludedIndex
	// 	if matchIndex < 0 {
	// 		matchIndex = 0
	// 	}
	// 	rf.matchIndex[i] = matchIndex
	// }
	// DPrintf("快照后nextIndex:%v", rf.nextIndex)
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	log := rf.log
	rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshot.LastIncludedIndex)
	e.Encode(snapshot.LastIncludedTerm)
	e.Encode(snapshot.Data)
	e.Encode(snapshot.SeqNums)
	e.Encode(snapshot.Shard2Data)
	e.Encode(snapshot.Shard2SeqNums)
	e.Encode(snapshot.Shard2Status)
	e.Encode(snapshot.ConfigNum)
	data := w.Bytes()

	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode(currentTerm)
	e1.Encode(votedFor)
	e1.Encode(log)
	state := w1.Bytes()
	rf.persister.SaveStateAndSnapshot(state, data)

}

func (rf *Raft) restoreSnapshot(data []byte) {
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	var snapshotData map[string]string
	var seqNums map[int64]int64
	var shard2Data map[int]map[string]string
	var shard2SeqNums map[int]map[int64]int64
	var shard2Status map[int]ShardKVStatus
	var configNum int
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&snapshotData) != nil ||
		d.Decode(&seqNums) != nil ||
		d.Decode(&shard2Data) != nil ||
		d.Decode(&shard2SeqNums) != nil ||
		d.Decode(&shard2Status) != nil ||
		d.Decode(&configNum) != nil {
		// error
		DPrintf("[ERROR] [Node:%d] Failed to read persisted snapshot", rf.me)
		panic("Failed to read persisted snapshot")
	} else {
		rf.lastSnapshot.LastIncludedTerm = lastIncludedTerm
		rf.lastSnapshot.LastIncludedIndex = lastIncludedIndex
		rf.lastSnapshot.Data = snapshotData
		rf.lastSnapshot.SeqNums = seqNums
		rf.lastSnapshot.Shard2Data = shard2Data
		rf.lastSnapshot.Shard2SeqNums = shard2SeqNums
		rf.lastSnapshot.Shard2Status = shard2Status
		rf.lastSnapshot.ConfigNum = configNum
		rf.logger.LogWithTrace(RPC_RECV, TraceContext{From: -1, To: rf.me},
			"从快照恢复持久化状态 lastIncludedTerm:%d lastIncludedIndex:%d snapshotData:%v shard2Status:%v configNum:%d",
			lastIncludedTerm, lastIncludedIndex, snapshotData, shard2Status, configNum)
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		// error
		DPrintf("[ERROR] [Node:%d] Failed to read persisted state", rf.me)
		panic("Failed to read persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logEntries
		rf.logger.LogWithTrace(RPC_RECV, TraceContext{From: -1, To: rf.me},
			"恢复持久化状态 currentTerm:%d votedFor:%d logLen:%d",
			currentTerm, votedFor, len(logEntries))
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Snapshot          Snapshot
	Term              int //leader’s term
	LeaderId          int // so follower can redirect clients
	LastSnapshotIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	// Your data here (2A).
	Term int //currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	trace := TraceContext{
		TraceID: "",
		From:    0,
		To:      rf.me,
	}
	rf.logger.LogWithTrace(RPC_RECV, trace, "InstallSnapshot 收到 args:%v", args)
	// Reply immediately if term < currentTerm
	// 如果 RPC 的 term < currentTerm，立即回复 false（拒绝），并返回自己的 currentTerm。
	if args.Term < rf.currentTerm {
		rf.logger.LogWithTrace(RPC_RECV, trace,
			"InstallSnapshot args.Term:%d rf.currentTerm:%d",
			args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// 重制选举定时器
	rf.lastHeartBeatTime.Store(time.Now())

	// If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	discardAll := false
	//  InstallSnapshot lastApplied:52 snapshot last included index:58 log [{<nil> 0 false 0} {{49  Get a6f2e3f51ceb8a6fad3c61770becb3f3-2188678353658944606} 3 false 59}]
	rf.logger.LogWithTrace(RPC_RECV, trace,
		"InstallSnapshot lastApplied:%d snapshot last included index:%d log %v",
		rf.lastApplied, rf.lastSnapshot.LastIncludedIndex, rf.log)
	if args.Snapshot.LastIncludedIndex <= rf.lastSnapshot.LastIncludedIndex {
		// 这是一个过时的 snapshot，忽略它
		rf.logger.LogWithTrace(RPC_RECV, trace,
			"InstallSnapshot 拒绝过时的snapshot lastIncludedIndex:%d <= lastApplied:%d",
			args.Snapshot.LastIncludedIndex, rf.lastSnapshot.LastIncludedIndex)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if rf.lastApplied < args.Snapshot.LastIncludedIndex {
		//  Discard the entire log
		// if rf.log[rf.getLastLogIndex()].Index > args.LastSnapshotIndex {
		// 	tempLog := make([]LogEntry, 0)
		// 	tempLog = append(rf.log, LogEntry{}) // 添加哨兵节点
		// 	rf.log = append(tempLog, rf.log[rf.getLogStartIndex(rf.getLastLogIndex()):]...)
		// } else {
		// 	tempLog := make([]LogEntry, 0)
		// 	tempLog = append(tempLog, NewLogEntry(rf.log[0].Command, args.Snapshot.LastIncludedTerm, 0)) // 添加哨兵节点
		// 	rf.log = tempLog
		// }
		rf.mu.Unlock()
		rf.logger.LogWithTrace(RPC_RECV, trace,
			"InstallSnapshot 通知上层KV server data:%v seqNums:%v",
			args.Snapshot.Data, args.Snapshot.SeqNums)
		// Reset state machine using snapshot contents (and load snapshot's cluster configuration)
		rf.applyCh <- ApplyMsg{
			Snapshot:      args.Snapshot.Data,
			IsSnapshot:    true,
			CommandIndex:  args.Snapshot.LastIncludedIndex,
			SeqNums:       args.Snapshot.SeqNums, // 包含seqNums以恢复幂等性状态
			CommandValid:  false,
			Shard2Data:    args.Snapshot.Shard2Data,
			Shard2SeqNums: args.Snapshot.Shard2SeqNums,
			Shard2Status:  args.Snapshot.Shard2Status,
			ConfigNum:     args.Snapshot.ConfigNum,
		}
		discardAll = true
	} else {
		rf.mu.Unlock()
		// if len(rf.log) == 1 {
		// 	rf.mu.Unlock()
		// 	return
		// } else {
		// 	if rf.log[1].Index > args.LastSnapshotIndex {
		// 		rf.mu.Unlock()
		// 		return
		// 	}
		// }
	}
	// rf.mu.Unlock()
	// Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.saveSnapshot(args.Snapshot, discardAll, args.LastSnapshotIndex)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int    // 候选人最后一条日志的 index（用于判断日志新旧）
	LastLogTerm  int    // 候选人最后一条日志的 term。
	TraceID      string // 添加追踪ID
	From         int    // 添加发送者节点ID
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 接收者的 currentTerm
	VoteGranted bool // 表示该 follower 是否给候选人投票。
}

// example RequestVote RPC handler.
// Reply false if term < currentTerm (§5.1)
// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log,
// grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	currentTerm := rf.currentTerm
	me := rf.me
	lastLogIndex := rf.log[rf.getLastLogIndex()].Index
	lastLogTerm := rf.log[rf.getLastLogIndex()].Term
	status := rf.status
	defer rf.mu.Unlock()
	// 检查是否满足条件 满足就投票
	// 创建追踪上下文
	trace := TraceContext{
		TraceID: args.TraceID,
		From:    args.From,
		To:      me,
	}

	rf.logger.LogWithTrace(RPC_RECV, trace,
		"RequestVote 接受 term:%d candidateId:%d lastLogIndex:%d lastLogTerm:%d currentTerm:%d status:%s",
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, currentTerm, getStatusString(status))
	// 如果 RPC 的 term < currentTerm，立即回复 false（拒绝），并返回自己的 currentTerm。
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}
	// currentTerm := rf.currentTerm
	if args.Term > currentTerm {
		// 如果 RPC 包含更大的 term，要更新并退化为 follower
		rf.becomeFollower(args.Term)
	}
	currentTerm = rf.currentTerm
	votedFor := rf.votedFor
	// 否则（term >= currentTerm）需要检查两点：
	// 本节点尚未在该 term 投票（votedFor == -1 或已投给 candidateId）；
	rf.logger.LogWithTrace(RPC_RECV, trace, "投票信息 votedFor:%d lastLogTerm:%d lastLogIndex:%d commitIndex:%d last snapshot index:%d", votedFor, lastLogTerm, lastLogIndex, rf.commitIndex, rf.lastSnapshot.LastIncludedIndex)
	if votedFor == -1 || votedFor == args.CandidateId {
		// 检查候选人是否包含所有已提交的日志条目
		// if !rf.isCandidateSafe(args.LastLogIndex, args.LastLogTerm) {
		// 	reply.VoteGranted = false
		// 	reply.Term = currentTerm
		// 	rf.logger.LogWithTrace(RPC_RECV, trace, "拒绝投票：候选人未包含所有已提交日志 candidateLastLogIndex:%d candidateLastLogTerm:%d commitIndex:%d",
		// 		args.LastLogIndex, args.LastLogTerm, rf.commitIndex)
		// 	return
		// }

		// 候选人的日志至少跟接收者的日志一样新（比较 lastLogTerm，若相同则比较 lastLogIndex）。
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			// 满足这两点则授予投票（voteGranted = true），并把 votedFor = candidateId（并在稳定存储上持久化）。
			rf.logger.LogWithTrace(RPC_RECV, trace, "投票给 candidate:%d term:%d", args.CandidateId, args.Term)
			rf.grantVote(args.CandidateId)
			reply.VoteGranted = true
			reply.Term = currentTerm
			// 重置选举定时器
			rf.lastHeartBeatTime.Store(time.Now())
			return
		}
	}
	rf.logger.LogWithTrace(RPC_RECV, trace, "投票失败兜底 candidate:%d term:%d", args.CandidateId, args.Term)
	reply.Term = currentTerm
	reply.VoteGranted = false
}

// 检查候选人是否包含所有已提交的日志条目
// 根据Raft论文5.4.1节的要求，候选人必须包含所有已提交的日志条目
// func (rf *Raft) isCandidateSafe(candidateLastLogIndex int, candidateLastLogTerm int) bool {
// 	// 基本检查：如果候选人日志太短，肯定不包含所有已提交日志
// 	if candidateLastLogIndex < rf.commitIndex {
// 		rf.logger.LogWithTrace(RPC_RECV, TraceContext{}, "安全检查失败：候选人日志太短 candidateLastLogIndex:%d < commitIndex:%d",
// 			candidateLastLogIndex, rf.commitIndex)
// 		return false
// 	}

// 	// 检查候选人是否至少和本节点一样新
// 	// 这是Raft论文中要求的投票限制的一部分
// 	lastLogIndex := rf.getLastLogIndex()
// 	lastLogTerm := rf.log[lastLogIndex].Term

// 	isUpToDate := candidateLastLogTerm > lastLogTerm ||
// 		(candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex)

// 	if !isUpToDate {
// 		rf.logger.LogWithTrace(RPC_RECV, TraceContext{}, "安全检查失败：候选人日志不够新 candidate(%d,%d) vs current(%d,%d)",
// 			candidateLastLogTerm, candidateLastLogIndex, lastLogTerm, lastLogIndex)
// 		return false
// 	}

// 	return true
// }

func (rf *Raft) grantVote(candidateId int) {
	rf.votedFor = candidateId
	rf.status = Follower
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// AppendEntries RPC handler.
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int // leader’s term
	LeaderId     int
	PrevLogIndex int        // Leader的Log里面的上一个LogIndex
	PrevLogTerm  int        // Leader的Log里面的上一个LogIndex的Term
	Entries      []LogEntry // log信息
	LeaderCommit int        // leader’s commitIndex
	TraceID      string     // 添加追踪ID
	From         int        // 添加发送者节点ID
	Snapshot     Snapshot   // 快照信息
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term     int // 接收者的 currentTerm
	Success  bool
	LogTerm  int // 用于快速恢复
	LogIndex int // 用于快速恢复
	LogLen   int // 用于快速恢复
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	status := rf.status
	me := rf.me

	// 创建追踪上下文
	trace := TraceContext{
		TraceID: args.TraceID,
		From:    args.From,
		To:      me,
	}

	isHeartbeat := len(args.Entries) == 0
	eventType := HEARTBEAT
	if !isHeartbeat {
		eventType = LOG_REPLICA
	}

	rf.logger.LogWithTrace(eventType, trace,
		"AppendEntries 收到 term:%d leaderId:%d prevLogIndex:%d prevLogTerm:%d entries:%d leaderCommit:%d isHeartbeat:%t currentTerm:%d status:%s log:%v",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, isHeartbeat, currentTerm, getStatusString(status), args.Entries)

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < currentTerm {
		rf.logger.LogWithTrace(eventType, trace, "Rejected: 收到心跳，但是term小于当前term term(%d) < currentTerm(%d)", args.Term, currentTerm)
		reply.Term = currentTerm
		reply.Success = false
		return
	}
	// 重制选举定时器
	rf.lastHeartBeatTime.Store(time.Now())
	if args.Term > currentTerm {
		// 更新term
		rf.becomeFollower(args.Term)
	}
	currentTerm = rf.currentTerm
	logLen := rf.getNextIndex()
	comimtIndex := rf.commitIndex
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if logLen <= args.PrevLogIndex {
		// 不包含这个Log 返回false
		// 代表对应的Log信息不存在
		// 失败
		rf.logger.LogWithTrace(eventType, trace, "Rejected: 收到LogEntry，但是log不全， prevLogIndex:%d logLen:%d", args.PrevLogIndex, logLen)
		// log.Printf("raft %d 收到LogEntry，但是log不全， log %v", rf.me, rf.log)
		reply.Term = currentTerm
		reply.Success = false
		// 这个是Follower中，对应任期号为XTerm的第一条Log条目的下标。
		reply.LogIndex = logLen
		// 将自己的任期号放在XTerm中。如果Follower在对应位置没有Log，那么这里会返回 -1。
		reply.LogTerm = -1
		//如果Follower在对应位置没有Log，那么XTerm会返回-1，XLen表示空白的Log下标。
		reply.LogLen = -1
		return
	}
	rf.logger.LogWithTrace(eventType, trace, "打印日志结果:%v ", rf.log)
	logEntry := rf.log[rf.getLogStartIndex(args.PrevLogIndex)]
	if logEntry.Term != args.PrevLogTerm {
		// 失败
		// rf.logger.LogWithTrace(eventType, trace, "Rejected: 收到LogEntry，但是term 不对 prevLogIndex:%d logLen:%d logEntry term:%d prevlogterm:%d", args.PrevLogIndex, logLen, logEntry.Term, args.PrevLogTerm)
		reply.Term = currentTerm
		reply.Success = false
		reply.LogIndex = 0
		reply.LogTerm = logEntry.Term
		for i, log := range rf.log {
			if log.Term == logEntry.Term {
				reply.LogIndex = i
				break
			}
		}
		reply.LogLen = -1
		rf.persist()
		rf.logger.LogWithTrace(eventType, trace, "Rejected: 收到LogEntry，但是term 不对 prevLogIndex:%d logLen:%d logEntry term:%d prevlogterm:%d LogIndex:%d", args.PrevLogIndex, logLen, logEntry.Term, args.PrevLogTerm, reply.LogIndex)
		return
	}
	// 3. 如果现有条目与新条目冲突(相同的索引但不同的条款)，删除现有的条目等 遵循它（§5.3
	// 新条目的目标 index 从 prevLogIndex+1 开始
	// 4. Append any new entries not already in the log 追加日志
	if len(args.Entries) > 0 {
		rf.logger.LogWithTrace(eventType, trace, "args logEntrys:%v", args.Entries)
		for i, entry := range args.Entries {
			rf.logger.LogWithTrace(eventType, trace, "entry.Index:%d rf.getNextIndex():%d", entry.Index, rf.getNextIndex())
			if entry.Index < rf.getNextIndex() {
				startIndex := rf.getLogStartIndex(entry.Index)
				rf.logger.LogWithTrace(eventType, trace, "startIndex:%d rf.log[startIndex].Term:%d entry.Term:%d", entry.Index, rf.log[startIndex].Term, entry.Term)
				if rf.log[startIndex].Term != entry.Term {
					// 说明数据不一样 冲突
					// 删除
					rf.log = rf.log[:startIndex]
					rf.log = append(rf.log, args.Entries[i:]...)
					rf.persist()
					break
				}
			} else {
				// a = [1,2,3,4,5] b = a[2:] 表示下标2开始到结束也就是3,4,5
				// index是真实index-1 是下标，在-1才是前一个下标
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		}
	}
	rf.logger.LogWithTrace(eventType, trace, "复制日志结果:%v ", rf.log)
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > comimtIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.getNextIndex()-1)
		rf.logger.LogWithTrace(LOG_COMMIT, trace, "收到LogEntry，并且Leader已经提交， 本地log %d->%d leaderCommit:%d lastApplied:%d", oldCommitIndex, rf.commitIndex, args.LeaderCommit, rf.lastApplied)
		// 找到没有提交的
		if rf.commitIndex > rf.lastApplied {
			rf.applyLogs(rf.log[rf.getLogStartIndex(rf.lastApplied+1):rf.getLogStartIndex(rf.commitIndex+1)])
		}
	}
	reply.Term = currentTerm
	reply.Success = true

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	me := rf.me
	isLeader := rf.status == Leader
	term := rf.currentTerm
	// 生成客户端请求追踪ID
	clientTraceID := fmt.Sprintf("CLIENT_%d_%d", me, time.Now().UnixNano())
	trace := TraceContext{
		TraceID: clientTraceID,
		From:    -1, // 客户端
		To:      me,
	}

	rf.logger.LogWithTrace(RPC_RECV, trace, "接收到日志命令:%v isLeader:%t term:%d", command, isLeader, term)
	if !isLeader {
		return -1, term, isLeader
	}

	// index从1开始 这是当前command在log中的下标
	index := rf.getNextIndex()
	// Your code here (2B).
	// leader写入日志，未提交状态
	logEntry := NewLogEntry(command, term, index)
	rf.log = append(rf.log, logEntry)
	rf.persist()
	// 复制日志到Follower
	// rf.needHeartbeat = true
	// rf.heartbeatCond.Signal()
	return index, term, isLeader
}

func (rf *Raft) getNextIndex() int {
	DPrintf("[Node:%d] getNextIndex len:%d last snapshot index:%d", rf.me, len(rf.log), rf.lastSnapshot.LastIncludedIndex)
	return len(rf.log) + rf.lastSnapshot.LastIncludedIndex
}

// 日志复制主逻辑
// 如果last log index≥nextIndex，则发送 AppendEntries RPC，日志项从nextIndex开始
func (rf *Raft) logReplication() {
	for !rf.killed() {
		rf.mu.Lock()
		// 准备心跳信息
		currentTerm := rf.currentTerm
		me := rf.me
		commitIndex := rf.commitIndex
		agreeCount := int32(1)
		// rf.logger.LogWithTrace(LOG_REPLICA, trace, "开始日志复制发送 term:%d log:%v majority:%d", currentTerm, rf.log, majority)
		// 生成心跳追踪ID
		heartbeatTraceID := fmt.Sprintf("HB_%d_%d", me, currentTerm)
		trace := TraceContext{
			TraceID: heartbeatTraceID,
			From:    me,
			To:      -1, // 广播心跳
		}

		// 只有leader才发送心跳，并且控制发送频率
		if rf.status == Leader {
			rf.logger.LogWithTrace(HEARTBEAT, trace, "开始发送心跳广播 status %s term:%d commitIndex:%d", getStatusString(rf.status), currentTerm, commitIndex)

			// 重置选举定时器
			rf.lastHeartBeatTime.Store(time.Now())
			rf.mu.Unlock()
			// 发送日志
			for i := range rf.peers {
				if i == me {
					continue
				}
				go rf.sendLogEntries(currentTerm, me, commitIndex, i, &agreeCount)
			}
		} else {
			rf.logger.LogWithTrace(HEARTBEAT, trace, "Follower等心跳 status %s term:%d commitIndex:%d", getStatusString(rf.status), currentTerm, commitIndex)

			rf.mu.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// [0, 1, 2]
// last log index = 2 len = 3
// len - 1 就是last log index
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLogStartIndex(index int) int {
	DPrintf("[Node:%d] getLogStartIndex index:%d last snapshot index:%d", rf.me, index, rf.lastSnapshot.LastIncludedIndex)
	res := index - rf.lastSnapshot.LastIncludedIndex
	if res < 0 {
		return 0
	}
	return res
}

func (rf *Raft) sendLogEntries(currentTerm int, me int, commitIndex int, peer int, agreeCount *int32) {
	if rf.killed() {
		return
	}

	// 生成日志复制追踪ID
	logReplicationTraceID := fmt.Sprintf("LOG_%d_%d", me, currentTerm)
	trace := TraceContext{
		TraceID: logReplicationTraceID,
		From:    me,
		To:      peer,
	}

	rf.mu.Lock()
	DPrintf("[Node:%d] nextIndex:%v", rf.me, rf.nextIndex)
	nextIdx := rf.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	if prevLogIndex < rf.lastSnapshot.LastIncludedIndex {
		// 假设 nil 1 2 3 4 5 6 7,快照了12345,nextIdx=4 prevLogIndex=3，说明这个Follower数据老旧，需要同步快照
		// 像Follower同步快照
		rf.logger.LogWithTrace(LOG_REPLICA, trace, "日志复快速恢复快照 for peer:%d starting prevLogIndex:%d lastSnapshotIndex:%d", peer, prevLogIndex, rf.lastSnapshot.LastIncludedIndex)
		args := InstallSnapshotArgs{
			Snapshot: rf.lastSnapshot,
			Term:     currentTerm,
			LeaderId: me,
		}
		rf.mu.Unlock()
		snapshotReply := InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(peer, &args, &snapshotReply)
		rf.mu.Lock()
		if ok {
			rf.logger.LogWithTrace(LOG_REPLICA, trace, "发送快照到 Node:%d 成功", peer)
			if snapshotReply.Term > currentTerm {
				rf.becomeFollower(snapshotReply.Term)
			}
			rf.nextIndex[peer] = rf.lastSnapshot.LastIncludedIndex + 1
			nextIdx = rf.nextIndex[peer]
			prevLogIndex = nextIdx - 1
		} else {
			rf.logger.LogWithTrace(LOG_REPLICA, trace, "发送快照到 Node:%d 失败", peer)
			rf.mu.Unlock()
			return
		}
	}
	// 状态检查：只有在当前节点是leader且term一致时才发送
	if rf.status != Leader || rf.currentTerm != currentTerm {
		rf.mu.Unlock()
		return
	}
	prevLogTerm := rf.log[rf.getLogStartIndex(prevLogIndex)].Term
	// 防止越界
	logEntries := make([]LogEntry, 0)
	if nextIdx < rf.getNextIndex() {
		// 复制
		logEntries = append([]LogEntry{}, rf.log[rf.getLogStartIndex(nextIdx):]...)
	}

	// 判断follower进度

	rf.mu.Unlock()
	args := AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries, // 心跳不携带日志
		LeaderCommit: commitIndex,
		TraceID:      logReplicationTraceID,
		From:         me,
	}
	rf.logger.LogWithTrace(LOG_REPLICA, trace, "发送日志or心跳到 Node:%d 开始 args:%v", peer, args)
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(peer, &args, &reply) {
		//If AppendEntries RPC received from new leader: convert to follower
		rf.mu.Lock()
		if reply.Term > currentTerm {
			rf.becomeFollower(reply.Term)
			rf.mu.Unlock()
			return
		}
		// 状态检查
		if currentTerm != rf.currentTerm || rf.status != Leader {
			rf.mu.Unlock()
			return
		}
		rf.logger.LogWithTrace(LOG_REPLICA, trace, "心跳返回 from peer:%d term:%d success:%t", peer, currentTerm, reply.Success)
		if reply.Success {
			if len(logEntries) > 0 {
				rf.logReplicationSuccess(agreeCount, peer, currentTerm, logEntries, commitIndex, prevLogIndex)
			}
			rf.mu.Unlock()
		} else {
			// 快速恢复日志
			rf.logger.LogWithTrace(LOG_REPLICA, trace, "日志复制返回失败，开始快速恢复 for peer:%d starting fast recovery replyLogIndex:%d", peer, reply.LogIndex)
			// 回退term
			rf.nextIndex[peer] = reply.LogIndex
			rf.mu.Unlock()
			rf.sendLogEntries(currentTerm, me, commitIndex, peer, agreeCount)
		}
	}
}

func (rf *Raft) applyLogs(logEntries []LogEntry) {
	// 检查空数组，避免 index out of range 错误
	if len(logEntries) == 0 {
		return
	}
	rf.lastApplied = logEntries[len(logEntries)-1].Index
	// rf.lastApplied = rf.getLastLogIndex() 使用这个会有问题，因为可能只提交了一部分日志 假设现在有10条日志 只提交了前5条 那么lastApplied应该是5 而不是10
	// 生成日志应用追踪ID
	applyTraceID := fmt.Sprintf("APPLY_%d_%d", rf.me, rf.lastApplied)
	trace := TraceContext{
		TraceID: applyTraceID,
		From:    rf.me,
		To:      -1, // 内部应用事件
	}
	rf.logger.LogWithTrace(LOG_COMMIT, trace, "Applying logs count:%d log:%v lastApplied:%d", len(logEntries), logEntries, rf.lastApplied)
	// log.Printf("raft %d lastApplied %d logEntries %v", rf.me, rf.lastApplied, logEntries)
	for _, v := range logEntries {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      v.Command,
			CommandIndex: v.Index,
			IsSnapshot:   false,
		}
		// rf.log[v.Index].Commit = true
	}

}

// func (rf *Raft) runLogApply() {

// }

func NewLogEntry(command interface{}, term int, index int) LogEntry {
	return LogEntry{
		Command: command,
		Term:    term,
		Commit:  false,
		Index:   index,
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) logReplicationSuccess(agreeCount *int32, peer int, currentTerm int, logEntries []LogEntry, commitIndex int, prevLogIndex int) {
	atomic.AddInt32(agreeCount, 1)
	// 生成日志复制追踪ID
	logReplicationTraceID := fmt.Sprintf("LOG_%d_%d", rf.me, currentTerm)
	trace := TraceContext{
		TraceID: logReplicationTraceID,
		From:    rf.me,
		To:      peer,
	}
	rf.logger.LogWithTrace(LOG_REPLICA, trace, "日志复制返回成功 from peer:%d term:%d agreeCount:%d log %v", peer, currentTerm, atomic.LoadInt32(agreeCount), logEntries)
	// 更新表示Follower已经有了这个日志
	lastIndex := len(logEntries) - 1
	if logEntries[lastIndex].Index <= rf.matchIndex[peer] {
		// 已经更新过了
		rf.logger.LogWithTrace(LOG_REPLICA, trace, "日志复制返回成功 但是已经更新过了 from peer:%d term:%d matchIndex:%d log last index:%d", peer, currentTerm, rf.matchIndex[peer], logEntries[lastIndex].Index)
		return
	}
	// rf.matchIndex[peer] = rf.getLastLogIndex()
	// rf.matchIndex[peer] = logEntries[lastIndex].Index
	rf.matchIndex[peer] = prevLogIndex + len(logEntries)
	// 更新下次要同步的index: 已经同步了N，因此下次同步N+1
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	rf.logger.LogWithTrace(LOG_REPLICA, trace, "更新后 server:%d matchIndex:%d nextIndex:%d", peer, rf.matchIndex[peer], rf.nextIndex[peer])
	//If there exists an N such that
	// N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N.
	majority := (len(rf.peers) + 1) / 2
	// lastLogIdx := rf.log[rf.getLastLogIndex()].Index
	for N := rf.getNextIndex() - 1; N > commitIndex; N-- {
		count := 1 // 自己一票
		for i, mi := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			if mi >= N {
				count++
			}
		}
		// 如果多数票
		if count >= majority && rf.log[rf.getLogStartIndex(N)].Term == currentTerm {
			// 多数同意，就提交
			commitTrace := TraceContext{
				TraceID: logReplicationTraceID,
				From:    rf.me,
				To:      -1, // 广播提交事件
			}
			rf.logger.LogWithTrace(LOG_COMMIT, commitTrace, "日志复制返回成功 多数同意，提交日志给KV服务器 index:%d term:%d majorityCount:%d", N, currentTerm, count)
			commitLog := rf.log[rf.getLogStartIndex(rf.commitIndex+1):rf.getLogStartIndex(N+1)]
			rf.commitIndex = N
			// 通知上层
			if rf.commitIndex > rf.lastApplied {
				rf.applyLogs(commitLog)
			}
			break
		}
	}
}

func (rf *Raft) becomeFollower(term int) {
	oldStatus := rf.status
	oldTerm := rf.currentTerm
	rf.logger.LogStateChange(oldStatus, Follower, oldTerm, "降级成follower")
	rf.currentTerm = term
	rf.votedFor = -1
	rf.status = Follower
	rf.persist()
}

// 广播选举
func (rf *Raft) broadcastVote(me int, currentTerm int, lastLogIndex int, lastLogTerm int) {
	voted := int32(0) // 自己的1票
	atomic.StoreInt32(&voted, 1)
	majority := (len(rf.peers) + 1) / 2
	// 生成选举追踪ID
	electionTraceID := fmt.Sprintf("ELEC_%d_%d", me, currentTerm)

	trace := TraceContext{
		TraceID: electionTraceID,
		From:    me,
		To:      -1, // 广播
	}
	// log.Printf("raft %d 票数 %d 多数节点 %d", me, voted, majority)
	for i := range rf.peers {
		if i == me {
			continue
		}
		go func(peer int) {
			if rf.killed() {
				return
			}
			args := RequestVoteArgs{}
			args.CandidateId = me
			args.Term = currentTerm
			args.LastLogIndex = lastLogIndex
			args.LastLogTerm = lastLogTerm
			args.TraceID = electionTraceID
			args.From = me

			reply := RequestVoteReply{}
			rf.logger.LogWithTrace(RPC_SEND, trace, "发送选举请求 peer:%d for term %d", peer, currentTerm)
			if rf.sendRequestVote(peer, &args, &reply) {
				// 处理返回值 如果获得多数票 则停止
				rf.mu.Lock()
				rf.logger.LogWithTrace(RPC_SEND, trace, "处理投票返回 for term %d reply:%v", rf.currentTerm, reply)
				// 如果返回的term更大，代表已经进入了下一个term选举
				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				// 状态检查
				if currentTerm != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				// If votes received from majority of servers: become leader
				// 如果收到多数票 —— 成为 leader；立即发送空 AppendEntries（心跳）以建立权威。
				if reply.VoteGranted {
					// log.Printf("raft %d received vote for term %d", rf.me, rf.currentTerm)
					// 原子+1
					atomic.AddInt32(&voted, 1)
					if atomic.LoadInt32(&voted) == int32(majority) {
						rf.becomeLeader(reply.Term)
					}
					// log.Printf("raft %d 当前票数 %d", rf.me, atomic.LoadInt32(&voted))
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader(term int) {
	rf.mu.Lock()
	// 状态检查
	if rf.status != Candidate || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	oldStatus := rf.status
	rf.logger.LogStateChange(oldStatus, Leader, term, "成为Leader")
	rf.status = Leader
	// 初始化nextIndex
	for i := range rf.peers {
		// rf.nextIndex[i] = rf.getLastLogIndex() + 1 + rf.lastSnapshot.LastIncludedIndex
		rf.nextIndex[i] = rf.getNextIndex()
		rf.matchIndex[i] = 0
	}
	rf.persist()
	rf.mu.Unlock()
	// 启动心跳goroutine
	// go rf.runHeartbeat()
}

func (rf *Raft) becomeCandidate() {
	if rf.status == Leader {
		return
	}
	oldStatus := rf.status
	oldTerm := rf.currentTerm
	rf.logger.LogStateChange(oldStatus, Follower, oldTerm, "变成Candidate")
	rf.currentTerm++
	rf.status = Candidate
	rf.votedFor = rf.me
	rf.persist()
}

// 选举定时器
func (rf *Raft) runElectionTimer() {
	// Sleep 随机毫秒数
	// 创建一个计时器
	// 定义随机超时范围（论文推荐 150-300ms）
	minTimeout := 150 * time.Millisecond
	maxTimeout := 300 * time.Millisecond
	// 生成定时器追踪ID
	timerTraceID := fmt.Sprintf("TIMER_%d", rf.me)
	trace := TraceContext{
		TraceID: timerTraceID,
		From:    -1, // 内部定时器事件
		To:      rf.me,
	}
	for !rf.killed() {
		timeout := randomTimeout(minTimeout, maxTimeout)
		since := time.Since(rf.lastHeartBeatTime.Load().(time.Time))
		// rf.mu.Lock()
		// status := rf.status
		// rf.mu.Unlock()
		// if status == Leader {
		// 	time.Sleep(10 * time.Millisecond) // 优化选举定时器检查频率，减少CPU消耗
		// 	continue
		// }
		if since > timeout {
			rf.mu.Lock()
			// log.Printf("raft %d since %v  Sleeping for timeout %v", rf.me, since, timeout)
			if rf.status == Leader {
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond) // 优化选举定时器检查频率，减少CPU消耗
				return
			}
			rf.becomeCandidate()
			me := rf.me
			currentTerm := rf.currentTerm
			lastLogIndex := rf.log[rf.getLastLogIndex()].Index
			lastLogTerm := rf.log[rf.getLastLogIndex()].Term
			// 超过超时时间
			rf.logger.LogWithTrace(TIMER_EVENT, trace, "超时选举 since:%v threshold:%v", since, timeout)
			// 重置选举定时器
			rf.lastHeartBeatTime.Store(time.Now())
			rf.mu.Unlock()
			// 给其他节点发送请求投票 RPC
			rf.broadcastVote(me, currentTerm, lastLogIndex, lastLogTerm)
		}
		time.Sleep(10 * time.Millisecond) // 优化选举定时器检查频率，减少CPU消耗
	}
}

// 创建 Raft 节点检查日志新旧
func makeRaftNode(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = Follower
	rf.lastHeartBeatTime.Store(time.Now())
	rf.applyCh = applyCh
	rf.log = append(rf.log, LogEntry{})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	// 初始化logger
	rf.logger = NewLogger(me)
	rf.heartbeatCond = sync.NewCond(&rf.mu)

	return rf
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// 创建raft节点
	rf := makeRaftNode(peers, me, persister, applyCh)
	// initialize from state persisted before a crash
	if persister.SnapshotSize() > 0 {
		rf.restoreSnapshot(persister.ReadSnapshot())
	}
	rf.readPersist(persister.ReadRaftState())

	// go channel 选举定时器
	go rf.runElectionTimer()
	// 日志通知
	go rf.logReplication()
	// log.Printf("raft %d started", me)
	return rf
}

// 随机生成一个超时时间
func randomTimeout(min, max time.Duration) time.Duration {
	delta := max - min
	return min + time.Duration(rand.Int63n(int64(delta)))
}

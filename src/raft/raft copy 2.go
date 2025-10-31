package raft

// import (
// 	"fmt"
// 	"log"
// 	"math/rand"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	"learn-6.824/src/labrpc"
// )

// //
// // this is an outline of the API that raft must expose to
// // the service (or tester). see comments below for
// // each of these functions for more details.
// //
// // rf = Make(...)
// //   create a new Raft server.
// // rf.Start(command interface{}) (index, term, isleader)
// //   start agreement on a new log entry
// // rf.GetState() (term, isLeader)
// //   ask a Raft for its current term, and whether it thinks it is leader
// // ApplyMsg
// //   each time a new entry is committed to the log, each Raft peer
// //   should send an ApplyMsg to the service (or tester)
// //   in the same server.
// //

// // import "bytes"
// // import "../labgob"

// // RaftStatus represents the state of a Raft node
// // as each Raft peer becomes aware that successive log entries are
// // committed, the peer should send an ApplyMsg to the service (or
// // tester) on the same server, via the applyCh passed to Make(). set
// // CommandValid to true to indicate that the ApplyMsg contains a newly
// // committed log entry.
// //
// // in Lab 3 you'll want to send other kinds of messages (e.g.,
// // snapshots) on the applyCh; at that point you can add fields to
// // ApplyMsg, but set CommandValid to false for these other uses.
// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int
// }

// // TraceContext 用于追踪请求链路
// type TraceContext struct {
// 	TraceID string
// 	From    int
// 	To      int
// }

// // EventType 事件类型
// type EventType string

// const (
// 	RPC_RECV       EventType = "RPC_RECV"
// 	RPC_SEND       EventType = "RPC_SEND"
// 	STATE_CHANGE   EventType = "STATE_CHANGE"
// 	LOG_COMMIT     EventType = "LOG_COMMIT"
// 	LOG_REPLICA    EventType = "LOG_REPLICA"
// 	TIMER_EVENT    EventType = "TIMER_EVENT"
// 	ELECTION_EVENT EventType = "ELECTION_EVENT"
// 	HEARTBEAT      EventType = "HEARTBEAT"
// )

// // Logger 结构化日志记录器
// type Logger struct {
// 	nodeId int
// }

// // NewLogger 创建新的日志记录器
// func NewLogger(nodeId int) *Logger {
// 	return &Logger{nodeId: nodeId}
// }

// // LogWithTrace 记录带追踪信息的日志
// func (l *Logger) LogWithTrace(eventType EventType, trace TraceContext, format string, args ...interface{}) {
// 	timestamp := time.Now().Format("15:04:05.000")

// 	message := fmt.Sprintf(format, args...)

// 	traceInfo := ""
// 	if trace.TraceID != "" {
// 		if trace.From >= 0 && trace.To >= 0 {
// 			traceInfo = fmt.Sprintf("[Trace:%s][%d->%d]", trace.TraceID, trace.From, trace.To)
// 		} else {
// 			traceInfo = fmt.Sprintf("[Trace:%s]", trace.TraceID)
// 		}
// 	}

// 	log.Printf("[%s] [Node:%d] [%s] %s %s",
// 		timestamp, l.nodeId, eventType, traceInfo, message)
// }

// // LogStateChange 记录状态变化
// func (l *Logger) LogStateChange(from, to RaftStatus, term int, reason string) {
// 	timestamp := time.Now().Format("15:04:05.000")

// 	log.Printf("[%s] [Node:%d] [%s] [STATE_CHANGE] %s->%s term:%d reason:%s",
// 		timestamp, l.nodeId, "INFO",
// 		getStatusString(from), getStatusString(to), term, reason)
// }

// // getStatusString 获取状态字符串
// func getStatusString(status RaftStatus) string {
// 	switch status {
// 	case Follower:
// 		return "FOLLOWER"
// 	case Candidate:
// 		return "CANDIDATE"
// 	case Leader:
// 		return "LEADER"
// 	default:
// 		return "UNKNOWN"
// 	}
// }

// // generateTraceID 生成追踪ID
// func generateTraceID() string {
// 	return fmt.Sprintf("%d%d", time.Now().UnixNano(), rand.Intn(1000))
// }

// type RaftStatus int

// const (
// 	Follower RaftStatus = iota
// 	Candidate
// 	Leader
// )

// type LogEntry struct {
// 	Command interface{}
// 	Term    int
// 	Commit  bool
// 	Index   int
// }

// // A Go object implementing a single Raft peer.
// // 实现单个Raft对等体的Go对象
// type Raft struct {
// 	mu        sync.Mutex          // Lock to protect shared access to this peer's state
// 	peers     []*labrpc.ClientEnd // RPC end points of all peers
// 	persister *Persister          // Object to hold this peer's persisted state
// 	me        int                 // this peer's index into peers[]
// 	dead      int32               // set by Kill()

// 	// 添加logger
// 	logger *Logger // 结构化日志记录器

// 	// Your data here (2A, 2B, 2C).
// 	// Look at the paper's Figure 2 for a description of what
// 	// state a Raft server must maintain.
// 	// 需要持久化
// 	currentTerm int        //当前任期
// 	votedFor    int        // 投票给谁
// 	log         []LogEntry // log信息

// 	// 不需要持久化
// 	commitIndex int // 已提交的最大索引
// 	lastApplied int // 已应用到状态机的最大索引

// 	// leader的属性
// 	nextIndex  []int // 对每个 follower，下次要发送给每个follower的日志索引
// 	matchIndex []int // 对每个 follower，对端已复制的最大日志索引

// 	// 节点状态
// 	status RaftStatus
// 	// 心跳时间
// 	lastHeartBeatTime atomic.Value
// 	// 通道
// 	applyCh chan ApplyMsg
// }

// // return currentTerm and whether this server
// // believes it is the leader.
// func (rf *Raft) GetState() (int, bool) {

// 	// var term int
// 	// var isleader bool
// 	// Your code here (2A).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	return rf.currentTerm, rf.status == Leader
// }

// // save Raft's persistent state to stable storage,
// // where it can later be retrieved after a crash and restart.
// // see paper's Figure 2 for a description of what should be persistent.
// func (rf *Raft) persist() {
// 	// Your code here (2C).
// 	// Example:
// 	// w := new(bytes.Buffer)
// 	// e := labgob.NewEncoder(w)
// 	// e.Encode(rf.xxx)
// 	// e.Encode(rf.yyy)
// 	// data := w.Bytes()
// 	// rf.persister.SaveRaftState(data)
// }

// // restore previously persisted state.
// func (rf *Raft) readPersist(data []byte) {
// 	if data == nil || len(data) < 1 { // bootstrap without any state?
// 		return
// 	}
// 	// Your code here (2C).
// 	// Example:
// 	// r := bytes.NewBuffer(data)
// 	// d := labgob.NewDecoder(r)
// 	// var xxx
// 	// var yyy
// 	// if d.Decode(&xxx) != nil ||
// 	//    d.Decode(&yyy) != nil {
// 	//   error...
// 	// } else {
// 	//   rf.xxx = xxx
// 	//   rf.yyy = yyy
// 	// }
// }

// // example RequestVote RPC arguments structure.
// // field names must start with capital letters!
// type RequestVoteArgs struct {
// 	// Your data here (2A, 2B).
// 	Term         int
// 	CandidateId  int
// 	LastLogIndex int    // 候选人最后一条日志的 index（用于判断日志新旧）
// 	LastLogTerm  int    // 候选人最后一条日志的 term。
// 	TraceID      string // 添加追踪ID
// 	From         int    // 添加发送者节点ID
// }

// // example RequestVote RPC reply structure.
// // field names must start with capital letters!
// type RequestVoteReply struct {
// 	// Your data here (2A).
// 	Term        int  // 接收者的 currentTerm
// 	VoteGranted bool // 表示该 follower 是否给候选人投票。
// }

// // example RequestVote RPC handler.
// // Reply false if term < currentTerm (§5.1)
// // If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log,
// // grant vote (§5.2, §5.4)
// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	// 创建追踪上下文
// 	trace := TraceContext{
// 		TraceID: args.TraceID,
// 		From:    args.From,
// 		To:      rf.me,
// 	}

// 	rf.logger.LogWithTrace(RPC_RECV, trace,
// 		"RequestVote 接受 term:%d candidateId:%d lastLogIndex:%d lastLogTerm:%d currentTerm:%d status:%s",
// 		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.currentTerm, getStatusString(rf.status))

// 	// 如果 RPC 的 term < currentTerm，立即回复 false（拒绝），并返回自己的 currentTerm。
// 	if args.Term < rf.currentTerm {
// 		rf.logger.LogWithTrace(RPC_RECV, trace,
// 			"Rejected: term(%d) < currentTerm(%d)", args.Term, rf.currentTerm)
// 		reply.Term = rf.currentTerm
// 		reply.VoteGranted = false
// 		return
// 	}

// 	if args.Term > rf.currentTerm {
// 		// 如果 RPC 包含更大的 term，要更新并退化为 follower
// 		oldStatus := rf.status
// 		rf.currentTerm = args.Term
// 		rf.votedFor = -1
// 		rf.status = Follower
// 		rf.logger.LogStateChange(oldStatus, Follower, rf.currentTerm, "received higher term in RequestVote")
// 	}

// 	// 检查是否可以投票
// 	canVote := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
// 	if canVote {
// 		// TODO: 这里应该添加日志新旧检查的逻辑，为简化示例暂时省略
// 		rf.logger.LogWithTrace(RPC_RECV, trace,
// 			"投票给 candidate:%d term:%d", args.CandidateId, args.Term)
// 		rf.votedFor = args.CandidateId
// 		reply.VoteGranted = true
// 		reply.Term = rf.currentTerm
// 		rf.status = Follower
// 		return
// 	}

// 	rf.logger.LogWithTrace(RPC_RECV, trace,
// 		"Vote denied: already voted for:%d", rf.votedFor)
// 	reply.Term = rf.currentTerm
// 	reply.VoteGranted = false
// }

// // example code to send a RequestVote RPC to a server.
// // server is the index of the target server in rf.peers[].
// // expects RPC arguments in args.
// // fills in *reply with RPC reply, so caller should
// // pass &reply.
// // the types of the args and reply passed to Call() must be
// // the same as the types of the arguments declared in the
// // handler function (including whether they are pointers).
// //
// // The labrpc package simulates a lossy network, in which servers
// // may be unreachable, and in which requests and replies may be lost.
// // Call() sends a request and waits for a reply. If a reply arrives
// // within a timeout interval, Call() returns true; otherwise
// // Call() returns false. Thus Call() may not return for a while.
// // A false return can be caused by a dead server, a live server that
// // can't be reached, a lost request, or a lost reply.
// //
// // Call() is guaranteed to return (perhaps after a delay) *except* if the
// // handler function on the server side does not return.  Thus there
// // is no need to implement your own timeouts around Call().
// //
// // look at the comments in ../labrpc/labrpc.go for more details.
// //
// // if you're having trouble getting RPC to work, check that you've
// // capitalized all field names in structs passed over RPC, and
// // that the caller passes the address of the reply struct with &, not
// // the struct itself.
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

// // AppendEntries RPC handler.
// type AppendEntriesArgs struct {
// 	// Your data here (2A, 2B).
// 	Term         int // leader's term
// 	LeaderId     int
// 	PrevLogIndex int        // Leader的Log里面的上一个LogIndex
// 	PrevLogTerm  int        // Leader的Log里面的上一个LogIndex的Term
// 	Entries      []LogEntry // log信息
// 	LeaderCommit int        // leader's commitIndex
// 	TraceID      string     // 添加追踪ID
// 	From         int        // 添加发送者节点ID
// }

// type AppendEntriesReply struct {
// 	// Your data here (2A).
// 	Term     int // 接收者的 currentTerm
// 	Success  bool
// 	LogTerm  int // 用于快速恢复
// 	LogIndex int // 用于快速恢复
// }

// // 1. Reply false if term < currentTerm (§5.1)
// // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
// // 3. If an existing entry conflicts with a new one (same index
// // but different terms), delete the existing entry and all that follow it (§5.3)
// // 4. Append any new entries not already in the log
// // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
// func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	rf.mu.Lock()

// 	// 创建追踪上下文
// 	trace := TraceContext{
// 		TraceID: args.TraceID,
// 		From:    args.From,
// 		To:      rf.me,
// 	}

// 	isHeartbeat := len(args.Entries) == 0
// 	eventType := HEARTBEAT
// 	if !isHeartbeat {
// 		eventType = LOG_REPLICA
// 	}

// 	rf.logger.LogWithTrace(eventType, trace,
// 		"AppendEntries 接受 term:%d leaderId:%d prevLogIndex:%d prevLogTerm:%d entries:%d leaderCommit:%d isHeartbeat:%t currentTerm:%d status:%s",
// 		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, isHeartbeat, rf.currentTerm, getStatusString(rf.status))

// 	// 重置心跳时间
// 	rf.lastHeartBeatTime.Store(time.Now())

// 	// 1. Reply false if term < currentTerm (§5.1)
// 	if args.Term < rf.currentTerm {
// 		rf.logger.LogWithTrace(eventType, trace,
// 			"Rejected: 收到心跳，但是term小于当前term term(%d) < currentTerm(%d)", args.Term, rf.currentTerm)
// 		reply.Term = rf.currentTerm
// 		reply.Success = false
// 		rf.mu.Unlock()
// 		return
// 	}

// 	// 处理term更新
// 	if args.Term > rf.currentTerm {
// 		oldStatus := rf.status
// 		rf.currentTerm = args.Term
// 		rf.votedFor = -1
// 		rf.status = Follower
// 		rf.logger.LogStateChange(oldStatus, Follower, rf.currentTerm, "received higher term in AppendEntries")
// 	}

// 	// 2. 检查日志一致性
// 	if len(rf.log) <= args.PrevLogIndex {
// 		rf.logger.LogWithTrace(eventType, trace,
// 			"Rejected: 收到LogEntry，但是log不全， prevLogIndex:%d logLen:%d", args.PrevLogIndex, len(rf.log))
// 		reply.Term = rf.currentTerm
// 		reply.Success = false
// 		// if len(rf.log) > 0 {
// 		reply.LogIndex = rf.commitIndex
// 		reply.LogTerm = rf.log[rf.commitIndex].Term
// 		// }
// 		rf.mu.Unlock()
// 		return
// 	}

// 	logEntry := rf.log[args.PrevLogIndex]
// 	if logEntry.Term != args.PrevLogTerm {
// 		rf.logger.LogWithTrace(eventType, trace,
// 			"Rejected: 收到LogEntry，但是term 不对 prevLogIndex:%d expected:%d actual:%d", args.PrevLogIndex, args.PrevLogTerm, logEntry.Term)
// 		reply.Term = rf.currentTerm
// 		reply.Success = false
// 		// 快速恢复逻辑
// 		for i, log := range rf.log {
// 			if log.Term == logEntry.Term {
// 				reply.LogIndex = i - 1
// 				break
// 			}
// 		}
// 		reply.LogTerm = logEntry.Term
// 		rf.mu.Unlock()
// 		return
// 	}

// 	// 4. 追加新日志
// 	if args.PrevLogIndex < len(rf.log)-1 {
// 		rf.log = rf.log[:args.PrevLogIndex+1]
// 	}
// 	rf.log = append(rf.log, args.Entries...)
// 	rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 		"复制日志结果：:%d newLogLen:%d", len(args.Entries), len(rf.log))

// 	// 5. 更新commitIndex
// 	if args.LeaderCommit > rf.commitIndex {
// 		oldCommitIndex := rf.commitIndex
// 		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
// 		rf.logger.LogWithTrace(LOG_COMMIT, trace,
// 			"收到LogEntry，并且Leader已经提交， 本地log %d->%d leaderCommit:%d", oldCommitIndex, rf.commitIndex, args.LeaderCommit)

// 		// 应用日志
// 		// if rf.commitIndex > rf.lastApplied {
// 		applyLogs := rf.log[rf.lastApplied+1:]
// 		rf.applyLogs(applyLogs)
// 		// }
// 	}

// 	reply.Term = rf.currentTerm
// 	reply.Success = true
// 	rf.mu.Unlock()
// }

// // the service using Raft (e.g. a k/v server) wants to start
// // agreement on the next command to be appended to Raft's log. if this
// // server isn't the leader, returns false. otherwise start the
// // agreement and return immediately. there is no guarantee that this
// // command will ever be committed to the Raft log, since the leader
// // may fail or lose an election. even if the Raft instance has been killed,
// // this function should return gracefully.
// //
// // the first return value is the index that the command will appear at
// // if it's ever committed. the second return value is the current
// // term. the third return value is true if this server believes it is
// // the leader.
// func (rf *Raft) Start(command interface{}) (int, int, bool) {
// 	rf.mu.Lock()
// 	me := rf.me
// 	isLeader := rf.status == Leader
// 	term := rf.currentTerm
// 	// index从1开始 这是当前command在log中的下标
// 	index := len(rf.log)

// 	// 生成客户端请求追踪ID
// 	clientTraceID := fmt.Sprintf("CLIENT_%d_%d", me, time.Now().UnixNano())
// 	trace := TraceContext{
// 		TraceID: clientTraceID,
// 		From:    -1, // 客户端
// 		To:      me,
// 	}

// 	rf.logger.LogWithTrace(RPC_RECV, trace,
// 		"接收到日志命令:%v isLeader:%t term:%d", command, isLeader, term)

// 	if !isLeader {
// 		rf.mu.Unlock()
// 		return -1, term, isLeader
// 	}

// 	// Your code here (2B).
// 	// leader写入日志，未提交状态
// 	logEntry := NewLogEntry(command, term, index)
// 	rf.log = append(rf.log, logEntry)

// 	// rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 	// 	"Leader added log entry index:%d term:%d command:%v", index, term, command)

// 	rf.mu.Unlock()
// 	// 复制日志到Follower
// 	rf.logReplication(rf.log)
// 	return index, term, isLeader
// }

// func (rf *Raft) logReplication(logs []LogEntry) {
// 	rf.mu.Lock()
// 	// 只有leader才发送日志
// 	if rf.status != Leader {
// 		rf.mu.Unlock()
// 		return
// 	}

// 	currentTerm := rf.currentTerm
// 	me := rf.me
// 	commitIndex := rf.commitIndex
// 	agreeCount := int32(1)
// 	majority := (len(rf.peers) + 1) / 2

// 	// 生成日志复制追踪ID
// 	logReplicationTraceID := fmt.Sprintf("LOG_%d_%d", me, currentTerm)
// 	trace := TraceContext{
// 		TraceID: logReplicationTraceID,
// 		From:    me,
// 		To:      -1, // 广播日志复制
// 	}

// 	rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 		"开始日志复制发送 term:%d logCount:%d majority:%d", currentTerm, len(logs), majority)

// 	rf.mu.Unlock()
// 	// 发送日志
// 	for i := range rf.peers {
// 		if i == me {
// 			continue
// 		}
// 		go rf.sendLogEntries(currentTerm, me, commitIndex, i, &agreeCount, majority)
// 	}
// }

// func (rf *Raft) sendLogEntries(currentTerm int, me int, commitIndex int, peer int, agreeCount *int32, majority int) {
// 	rf.mu.Lock()
// 	prevLogIndex := rf.nextIndex[peer] - 1
// 	prevLogTerm := rf.log[prevLogIndex].Term
// 	logEntries := rf.log[rf.nextIndex[peer]:]

// 	// 生成日志复制追踪ID
// 	logReplicationTraceID := fmt.Sprintf("LOG_%d_%d", me, currentTerm)
// 	trace := TraceContext{
// 		TraceID: logReplicationTraceID,
// 		From:    me,
// 		To:      peer,
// 	}

// 	// 判断follower进度
// 	if rf.log[len(rf.log)-1].Index < rf.nextIndex[peer] {
// 		rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 			"Server 已经同步了，不再发送 lastLogIndex:%d nextIndex:%d - skipping",
// 			rf.log[len(rf.log)-1].Index, rf.nextIndex[peer])
// 		rf.mu.Unlock()
// 		return
// 	}

// 	// rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 	// 	"Sending log entries prevLogIndex:%d prevLogTerm:%d entriesCount:%d commitIndex:%d",
// 	// 	prevLogIndex, prevLogTerm, len(logEntries), commitIndex)

// 	rf.mu.Unlock()
// 	args := AppendEntriesArgs{}
// 	args.Term = currentTerm
// 	args.LeaderId = me
// 	args.PrevLogIndex = prevLogIndex
// 	args.PrevLogTerm = prevLogTerm
// 	args.Entries = logEntries
// 	args.LeaderCommit = commitIndex
// 	args.TraceID = logReplicationTraceID
// 	args.From = me
// 	reply := AppendEntriesReply{}
// 	if rf.sendAppendEntries(peer, &args, &reply) {
// 		rf.mu.Lock()
// 		//If AppendEntries RPC received from new leader: convert to follower
// 		if reply.Term > rf.currentTerm {
// 			oldStatus := rf.status
// 			rf.currentTerm = reply.Term
// 			rf.votedFor = -1
// 			rf.status = Follower
// 			rf.logger.LogStateChange(oldStatus, Follower, rf.currentTerm, "received higher term in log replication reply 降级成为follower")
// 			rf.mu.Unlock()
// 			return
// 		}
// 		// 状态检查
// 		if currentTerm != rf.currentTerm || rf.status != Leader {
// 			rf.mu.Unlock()
// 			return
// 		}

// 		rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 			"日志复制返回 from peer:%d term:%d success:%t", peer, currentTerm, reply.Success)

// 		if reply.Success {
// 			atomic.AddInt32(agreeCount, 1)
// 			rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 				"日志复制返回成功 from peer:%d term:%d agreeCount:%d", peer, currentTerm, atomic.LoadInt32(agreeCount))
// 			// 更新表示Follower已经有了这个日志
// 			rf.matchIndex[peer] = logEntries[len(logEntries)-1].Index
// 			// 更新下次要同步的index: 已经同步了N，因此下次同步N+1
// 			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
// 			rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 				"更新后 server:%d matchIndex:%d nextIndex:%d", peer, rf.matchIndex[peer], rf.nextIndex[peer])

// 			//If there exists an N such that
// 			// N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// 			// set commitIndex = N.
// 			for N := len(rf.log); N > rf.commitIndex; N-- {
// 				count := 1 // 自己一票
// 				for i, mi := range rf.matchIndex {
// 					if i == rf.me {
// 						continue
// 					}
// 					if mi >= N {
// 						count++
// 					}
// 				}
// 				// 如果多数票
// 				if count >= majority && rf.log[N].Term == currentTerm {
// 					// 多数同意，就提交
// 					commitTrace := TraceContext{
// 						TraceID: logReplicationTraceID,
// 						From:    me,
// 						To:      -1, // 广播提交事件
// 					}
// 					rf.logger.LogWithTrace(LOG_COMMIT, commitTrace,
// 						"日志复制返回成功 多数同意，提交日志给KV服务器 index:%d term:%d majorityCount:%d", N, currentTerm, count)
// 					commitLog := rf.log[rf.commitIndex+1 : N+1]
// 					rf.commitIndex = N
// 					// 通知上层
// 					rf.applyLogs(commitLog)
// 					break
// 				}
// 			}
// 			rf.mu.Unlock()
// 		} else {
// 			// 快速恢复日志
// 			rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 				"日志复制返回失败，开始快速恢复 for peer:%d starting fast recovery replyLogIndex:%d", peer, reply.LogIndex)
// 			// 回退term
// 			rf.nextIndex[peer] = reply.LogIndex + 1
// 			rf.mu.Unlock()
// 			rf.sendLogEntries(currentTerm, me, commitIndex, peer, agreeCount, majority)
// 		}
// 	} else {
// 		rf.logger.LogWithTrace(LOG_REPLICA, trace,
// 			"Log replication RPC failed to peer:%d", peer)
// 	}
// }

// func (rf *Raft) applyLogs(logEntries []LogEntry) {
// 	rf.lastApplied = logEntries[len(logEntries)-1].Index

// 	// 生成日志应用追踪ID
// 	applyTraceID := fmt.Sprintf("APPLY_%d_%d", rf.me, rf.lastApplied)
// 	trace := TraceContext{
// 		TraceID: applyTraceID,
// 		From:    rf.me,
// 		To:      -1, // 内部应用事件
// 	}

// 	rf.logger.LogWithTrace(LOG_COMMIT, trace,
// 		"Applying logs count:%d lastApplied:%d", len(logEntries), rf.lastApplied)

// 	for _, v := range logEntries {
// 		rf.applyCh <- ApplyMsg{
// 			CommandValid: true,
// 			Command:      v.Command,
// 			CommandIndex: v.Index,
// 		}
// 		rf.log[v.Index].Commit = true
// 	}
// }

// func NewLogEntry(command interface{}, term int, index int) LogEntry {
// 	return LogEntry{
// 		Command: command,
// 		Term:    term,
// 		Commit:  false,
// 		Index:   index,
// 	}
// }

// // the tester doesn't halt goroutines created by Raft after each test,
// // but it does call the Kill() method. your code can use killed() to
// // check whether Kill() has been called. the use of atomic avoids the
// // need for a lock.
// //
// // the issue is that long-running goroutines use memory and may chew
// // up CPU time, perhaps causing later tests to fail and generating
// // confusing debug output. any goroutine with a long-running loop
// // should call killed() to check whether it should stop.
// func (rf *Raft) Kill() {
// 	atomic.StoreInt32(&rf.dead, 1)
// 	// Your code here, if desired.
// }

// func (rf *Raft) killed() bool {
// 	z := atomic.LoadInt32(&rf.dead)
// 	return z == 1
// }

// // 心跳
// func (rf *Raft) broadcastAppendEntries() {
// 	for !rf.killed() {
// 		rf.mu.Lock()
// 		// 只有leader才发送心跳
// 		if rf.status != Leader {
// 			rf.mu.Unlock()
// 			return
// 		}
// 		// 重置心跳时间
// 		rf.lastHeartBeatTime.Store(time.Now())
// 		currentTerm := rf.currentTerm
// 		me := rf.me
// 		commitIndex := rf.commitIndex

// 		// 生成心跳追踪ID
// 		heartbeatTraceID := fmt.Sprintf("HB_%d_%d", me, currentTerm)
// 		trace := TraceContext{
// 			TraceID: heartbeatTraceID,
// 			From:    me,
// 			To:      -1, // 广播心跳
// 		}

// 		rf.logger.LogWithTrace(HEARTBEAT, trace,
// 			"开始发送心跳广播 term:%d commitIndex:%d", currentTerm, commitIndex)

// 		rf.mu.Unlock()
// 		// 发送心跳
// 		for i := range rf.peers {
// 			if i == rf.me {
// 				continue
// 			}
// 			rf.mu.Lock()
// 			prevLogIndex := rf.nextIndex[i] - 1
// 			prevLogTerm := rf.log[prevLogIndex].Term
// 			rf.mu.Unlock()
// 			go func(peer int, prevIndex int, prevTerm int, hbTraceID string) {
// 				peerTrace := TraceContext{
// 					TraceID: hbTraceID,
// 					From:    me,
// 					To:      peer,
// 				}

// 				rf.logger.LogWithTrace(HEARTBEAT, peerTrace,
// 					"发送心跳 to peer:%d term:%d", peer, currentTerm)

// 				args := AppendEntriesArgs{}
// 				args.Term = currentTerm
// 				args.LeaderId = me
// 				args.PrevLogIndex = prevIndex
// 				args.PrevLogTerm = prevTerm
// 				args.Entries = nil
// 				args.LeaderCommit = commitIndex
// 				args.TraceID = heartbeatTraceID
// 				args.From = me
// 				reply := AppendEntriesReply{}
// 				if rf.sendAppendEntries(peer, &args, &reply) {
// 					rf.mu.Lock()
// 					//If AppendEntries RPC received from new leader: convert to follower
// 					if reply.Term > rf.currentTerm {
// 						oldStatus := rf.status
// 						rf.currentTerm = reply.Term
// 						rf.votedFor = -1
// 						rf.status = Follower
// 						rf.logger.LogStateChange(oldStatus, Follower, rf.currentTerm, "降级成为follower")
// 						rf.mu.Unlock()
// 						return
// 					}
// 					// 状态检查
// 					if currentTerm != rf.currentTerm || rf.status != Leader {
// 						rf.mu.Unlock()
// 						return
// 					}
// 					rf.logger.LogWithTrace(HEARTBEAT, peerTrace,
// 						"心跳返回 peer:%d term:%d success:%t", peer, currentTerm, reply.Success)
// 					rf.mu.Unlock()
// 				} else {
// 					rf.logger.LogWithTrace(HEARTBEAT, peerTrace,
// 						"Heartbeat RPC failed to peer:%d", peer)
// 				}
// 			}(i, prevLogIndex, prevLogTerm, heartbeatTraceID)
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

// // 广播选举
// func (rf *Raft) broadcastVote(me int, currentTerm int, commitIndex int) {
// 	// 生成选举追踪ID
// 	electionTraceID := fmt.Sprintf("ELEC_%d_%d", me, currentTerm)

// 	voted := int32(0) // 自己的1票
// 	atomic.StoreInt32(&voted, 1)
// 	majority := (len(rf.peers) + 1) / 2

// 	trace := TraceContext{
// 		TraceID: electionTraceID,
// 		From:    me,
// 		To:      -1, // 广播
// 	}

// 	rf.logger.LogWithTrace(ELECTION_EVENT, trace,
// 		"Broadcasting votes term:%d majority:%d currentVotes:%d", currentTerm, majority, atomic.LoadInt32(&voted))

// 	for i := range rf.peers {
// 		if i == me {
// 			continue
// 		}
// 		go func(peer int) {
// 			args := RequestVoteArgs{}
// 			args.CandidateId = me
// 			args.Term = currentTerm
// 			args.LastLogIndex = commitIndex
// 			args.LastLogTerm = 0
// 			args.TraceID = electionTraceID
// 			args.From = me

// 			peerTrace := TraceContext{
// 				TraceID: electionTraceID,
// 				From:    me,
// 				To:      peer,
// 			}

// 			rf.logger.LogWithTrace(RPC_SEND, peerTrace,
// 				"发送选举请求 to peer:%d term:%d lastLogIndex:%d", peer, currentTerm, commitIndex)

// 			reply := RequestVoteReply{}
// 			if rf.sendRequestVote(peer, &args, &reply) {
// 				// 处理返回值 如果获得多数票 则停止
// 				rf.mu.Lock()

// 				rf.logger.LogWithTrace(RPC_RECV, peerTrace,
// 					"处理投票返回 peer:%d term:%d voteGranted:%t replyTerm:%d",
// 					peer, currentTerm, reply.VoteGranted, reply.Term)

// 				// 如果返回的term更大，代表已经进入了下一个term选举
// 				if reply.Term > currentTerm {
// 					oldStatus := rf.status
// 					rf.currentTerm = reply.Term
// 					rf.votedFor = -1
// 					rf.status = Follower
// 					rf.logger.LogStateChange(oldStatus, Follower, rf.currentTerm, "降级成为Follower")
// 					rf.mu.Unlock()
// 					return
// 				}
// 				// 状态检查
// 				if rf.status != Candidate || rf.currentTerm != currentTerm {
// 					rf.mu.Unlock()
// 					return
// 				}
// 				// If votes received from majority of servers: become leader
// 				// 如果收到多数票 —— 成为 leader；立即发送空 AppendEntries（心跳）以建立权威。
// 				if reply.VoteGranted {
// 					// rf.logger.LogWithTrace(ELECTION_EVENT, peerTrace,
// 					// 	"Vote granted from peer:%d term:%d currentVotes:%d", peer, currentTerm, atomic.LoadInt32(&voted))
// 					// 原子+1
// 					atomic.AddInt32(&voted, 1)
// 					rf.logger.LogWithTrace(ELECTION_EVENT, trace,
// 						"当前获得投票 term:%d currentVotes:%d majority:%d", currentTerm, atomic.LoadInt32(&voted), majority)

// 					if atomic.LoadInt32(&voted) >= int32(majority) {
// 						oldStatus := rf.status
// 						rf.status = Leader
// 						rf.logger.LogStateChange(oldStatus, Leader, rf.currentTerm, "成为Leader")

// 						// 初始化nextIndex
// 						for i := range rf.peers {
// 							rf.nextIndex[i] = len(rf.log)
// 						}
// 						// 立即发送空 AppendEntries（心跳）以建立权威。
// 						rf.mu.Unlock()
// 						go rf.broadcastAppendEntries()
// 					} else {
// 						rf.mu.Unlock()
// 					}
// 				} else {
// 					rf.logger.LogWithTrace(ELECTION_EVENT, peerTrace,
// 						"Vote denied from peer:%d term:%d", peer, currentTerm)
// 					rf.mu.Unlock()
// 				}
// 			} else {
// 				rf.logger.LogWithTrace(RPC_SEND, peerTrace,
// 					"RequestVote RPC failed to peer:%d", peer)
// 			}
// 		}(i)
// 	}
// }

// // • Increment currentTerm
// // • Vote for self
// // • Reset election timer
// // • Send RequestVote RPCs to all other servers
// func (rf *Raft) Election() {
// 	rf.mu.Lock()
// 	if rf.status == Leader {
// 		rf.mu.Unlock()
// 		return
// 	}

// 	oldStatus := rf.status
// 	oldTerm := rf.currentTerm
// 	rf.currentTerm++
// 	rf.votedFor = rf.me
// 	rf.status = Candidate
// 	me := rf.me
// 	currentTerm := rf.currentTerm
// 	commitIndex := rf.commitIndex
// 	// 重置选举定时器
// 	rf.lastHeartBeatTime.Store(time.Now())

// 	// 生成选举追踪ID
// 	electionTraceID := fmt.Sprintf("ELEC_%d_%d", me, currentTerm)
// 	trace := TraceContext{
// 		TraceID: electionTraceID,
// 		From:    me,
// 		To:      -1,
// 	}

// 	rf.logger.LogStateChange(oldStatus, Candidate, rf.currentTerm, "选举超时")
// 	rf.logger.LogWithTrace(ELECTION_EVENT, trace,
// 		"变成候选人开始选举 term:%d->%d votedFor:%d", oldTerm, currentTerm, rf.votedFor)

// 	rf.mu.Unlock()
// 	// 给其他节点发送请求投票 RPC
// 	rf.broadcastVote(me, currentTerm, commitIndex)
// }

// // 选举定时器
// func (rf *Raft) runElectionTimer() {
// 	// Sleep 随机毫秒数
// 	// 创建一个计时器
// 	// 定义随机超时范围（论文推荐 150-300ms）
// 	minTimeout := 200 * time.Millisecond
// 	maxTimeout := 400 * time.Millisecond

// 	// 生成定时器追踪ID
// 	timerTraceID := fmt.Sprintf("TIMER_%d", rf.me)
// 	trace := TraceContext{
// 		TraceID: timerTraceID,
// 		From:    -1, // 内部定时器事件
// 		To:      rf.me,
// 	}

// 	rf.logger.LogWithTrace(TIMER_EVENT, trace,
// 		"选举定时启动 minTimeout:%v maxTimeout:%v", minTimeout, maxTimeout)

// 	for !rf.killed() {
// 		timeout := randomTimeout(minTimeout, maxTimeout)
// 		since := time.Since(rf.lastHeartBeatTime.Load().(time.Time))

// 		// rf.logger.LogWithTrace(TIMER_EVENT, trace,
// 		// "Timer check sinceLastHeartbeat:%v timeoutThreshold:%v", since, timeout)

// 		if since > timeout {
// 			// 超过超时时间
// 			rf.logger.LogWithTrace(TIMER_EVENT, trace,
// 				"超时选举 since:%v threshold:%v status:%s", since, timeout, getStatusString(rf.status))
// 			rf.Election()
// 		}
// 		time.Sleep(20 * time.Microsecond)
// 	}
// }

// // 创建 Raft 节点检查日志新旧
// func makeRaftNode(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
// 	rf := &Raft{}
// 	rf.peers = peers
// 	rf.persister = persister
// 	rf.me = me
// 	rf.currentTerm = 0
// 	rf.votedFor = -1
// 	rf.commitIndex = 0
// 	rf.lastApplied = 0
// 	rf.status = Follower
// 	rf.lastHeartBeatTime.Store(time.Now())
// 	rf.applyCh = applyCh
// 	rf.log = append(rf.log, LogEntry{})
// 	rf.nextIndex = make([]int, len(peers))
// 	rf.matchIndex = make([]int, len(peers))

// 	// 初始化logger
// 	rf.logger = NewLogger(me)

// 	return rf
// }

// // the service or tester wants to create a Raft server. the ports
// // of all the Raft servers (including this one) are in peers[]. this
// // server's port is peers[me]. all the servers' peers[] arrays
// // have the same order. persister is a place for this server to
// // save its persistent state, and also initially holds the most
// // recent saved state, if any. applyCh is a channel on which the
// // tester or service expects Raft to send ApplyMsg messages.
// // Make() must return quickly, so it should start goroutines
// // for any long-running work.
// func Make(peers []*labrpc.ClientEnd, me int,
// 	persister *Persister, applyCh chan ApplyMsg) *Raft {

// 	// 创建raft节点
// 	rf := makeRaftNode(peers, me, persister, applyCh)
// 	// initialize from state persisted before a crash
// 	rf.readPersist(persister.ReadRaftState())

// 	// go channel 选举定时器
// 	go rf.runElectionTimer()
// 	log.Printf("raft %d started", me)
// 	return rf
// }

// // 随机生成一个超时时间
// func randomTimeout(min, max time.Duration) time.Duration {
// 	delta := max - min
// 	return min + time.Duration(rand.Int63n(int64(delta)))
// }

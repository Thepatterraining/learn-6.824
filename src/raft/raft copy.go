package raft

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

// import (
// 	"sync"
// 	"sync/atomic"
// 	"time"
// 	"log"
// 	"math/rand"

// 	"learn-6.824/src/labrpc"
// )

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

// type RaftStatus int

// const (
// 	Follower RaftStatus = iota
// 	Candidate
// 	Leader
// )

// type ElectionTimer struct {
// 	min 	time.Duration
// 	max 	time.Duration
// 	timer 	*time.Timer
// 	timeout time.Duration
// }

// func (et *ElectionTimer) reset() {
// 	if !et.timer.Stop() {
// 		// 如果定时器已经触发了，要把 channel 清空一下
// 		select{
// 		case <- et.timer.C:
// 		default:
// 		}
// 	}
// 	log.Printf("重置定时器 %d", et.timeout)
// 	// 重置定时器
// 	timeout := randomTimeout(et.min, et.max)
// 	et.timer.Reset(timeout)
// }

// // A Go object implementing a single Raft peer.
// // 实现单个Raft对等体的Go对象
// type Raft struct {
// 	mu        sync.Mutex          // Lock to protect shared access to this peer's state
// 	peers     []*labrpc.ClientEnd // RPC end points of all peers
// 	persister *Persister          // Object to hold this peer's persisted state
// 	me        int                 // this peer's index into peers[]
// 	dead      int32               // set by Kill()

// 	// Your data here (2A, 2B, 2C).
// 	// Look at the paper's Figure 2 for a description of what
// 	// state a Raft server must maintain.
// 	// 需要持久化
// 	currentTerm int      //当前任期
// 	votedFor    int      // 投票给谁
// 	log         []string // log信息

// 	// 不需要持久化
// 	commitIndex int // 已提交的最大索引
// 	lastApplied int // 已应用到状态机的最大索引

// 	// leader的属性
// 	nextIndex  []int  // 下次要发送给每个follower的日志索引
// 	matchIndex []int // 对端已复制的最大日志索引

// 	// 节点状态
// 	status RaftStatus
// 	// 选举定时器
// 	electionTimer ElectionTimer
// 	resetCh chan struct{}
// }

// // return currentTerm and whether this server
// // believes it is the leader.
// func (rf *Raft) GetState() (int, bool) {

// 	// var term int
// 	// var isleader bool
// 	// Your code here (2A).
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
// 	LastLogIndex int // 候选人最后一条日志的 index（用于判断日志新旧）
// 	LastLogTerm  int // 候选人最后一条日志的 term。
// }

// // example RequestVote RPC reply structure.
// // field names must start with capital letters!
// type RequestVoteReply struct {
// 	// Your data here (2A).
// 	Term        int // 接收者的 currentTerm
// 	VoteGranted bool // 表示该 follower 是否给候选人投票。
// }

// // example RequestVote RPC handler.
// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	// Your code here (2A, 2B).
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// 检查是否满足条件 满足就投票
// 	log.Printf("raft %d received RequestVote from %d for term %d, args:%v, raft:%v", rf.me, args.CandidateId, args.Term, args, rf)
// 	// 如果 RPC 的 term < currentTerm，立即回复 false（拒绝），并返回自己的 currentTerm。
// 	if args.Term < rf.currentTerm {
// 		reply.Term = rf.currentTerm
// 		reply.VoteGranted = false
// 		return
// 	} else if args.Term > rf.currentTerm {
// 		// 如果 RPC 包含更大的 term，要更新并退化为 follower
// 		rf.currentTerm = args.Term
// 		rf.votedFor = -1
// 		rf.status = Follower
// 	}
// 	// 收到相同 term，但已投给别人 拒绝投票
// 	if args.Term == rf.currentTerm && rf.votedFor != -1 {
// 		log.Printf("raft %d 收到相同 term，但已投给别人 拒绝投票 投票给 %d", rf.me, rf.votedFor)
// 		reply.VoteGranted = false
// 		reply.Term = rf.currentTerm
// 		return
// 	}
// 	// log.Printf("raft %d checking vote for %d for term %d", rf.me, args.CandidateId, args.Term)
// 	// 否则（term >= currentTerm）需要检查两点：
// 	// 本节点尚未在该 term 投票（votedFor == -1 或已投给 candidateId）；
// 	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
// 		log.Printf("raft %d 检查日志新旧 for %d for term %d", rf.me, args.CandidateId, args.Term)
// 		// 候选人的日志至少跟接收者的日志一样新（比较 lastLogTerm，若相同则比较 lastLogIndex）。
// 		if args.LastLogTerm == rf.currentTerm && args.LastLogIndex >= rf.commitIndex {
// 			log.Printf("raft %d 投票给 %d for term %d", rf.me, args.CandidateId, args.Term)
// 			// 满足这两点则授予投票（voteGranted = true），并把 votedFor = candidateId（并在稳定存储上持久化）。
// 			rf.votedFor = args.CandidateId
// 			rf.status = Follower
// 			reply.VoteGranted = true
// 			reply.Term = rf.currentTerm
// 			return
// 		}
// 	}

// 	log.Printf("兜底")
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

// // AppendEntries RPC handler.
// type AppendEntriesArgs struct {
// 	// Your data here (2A, 2B).
// 	Term         int  // leader’s term
// 	LeaderId  	 int
// 	PrevLogIndex int // Leader的Log里面的上一个LogIndex
// 	PrevLogTerm  int // Leader的Log里面的上一个LogIndex的Term
// 	Entries		 []string // log信息
// 	LeaderCommit int // leader’s commitIndex
// }

// type AppendEntriesReply struct {
// 	// Your data here (2A).
// 	Term        int // 接收者的 currentTerm
// 	Success 	bool
// }

// func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// 判断，如果 entries 是空，代表心跳或者leader权威
// 	if (args.Entries == nil || len(args.Entries) == 0) {
// 		log.Printf("raft %d received heartbeat from %d for term %d, args:%v, raft:%v", rf.me, args.LeaderId, args.Term, args, rf)
// 		if (args.Term > rf.currentTerm) {
// 			// 更新term
// 			rf.currentTerm = args.Term
// 			rf.votedFor = -1
// 		}
// 		rf.status = Follower
// 		// 重置 选举计时器
// 		select {
// 		case rf.resetCh <- struct{}{}:
// 		default:
// 		}
// 		reply.Term = rf.currentTerm
// 		reply.Success = true
// 		return
// 	}
// 	if (args.Term < rf.currentTerm) {
// 		reply.Term = rf.currentTerm
// 		reply.Success = false
// 		return
// 	}

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
// 	index := -1
// 	term := -1
// 	isLeader := true

// 	// Your code here (2B).

// 	return index, term, isLeader
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

// // 广播Entry
// func (rf *Raft) broadcastAppendEntries() {
// 	for i, client := range rf.peers {
// 		if (i == rf.me) {
// 			continue
// 		}
// 		args := AppendEntriesArgs{}
// 		args.Term = rf.currentTerm
// 		args.LeaderId = rf.me
// 		args.PrevLogIndex = 0
// 		args.PrevLogTerm = 0
// 		args.Entries = nil
// 		args.LeaderCommit = rf.commitIndex
// 		reply := AppendEntriesReply{}
// 		client.Call("Raft.AppendEntries", &args, &reply)
// 		log.Printf("raft %d 广播Entry for term %d reply:%v", rf.me, rf.currentTerm, reply)
// 	}
// }

// // 广播选举
// func (rf *Raft) broadcastVote() {
// 	voted := 1 // 自己的1票
// 	majority := (len(rf.peers) + 1) / 2
// 	for i := range rf.peers {
// 		if (i == rf.me) {
// 			continue
// 		}
// 		args := RequestVoteArgs{}
// 		args.CandidateId = rf.me
// 		args.Term = rf.currentTerm
// 		args.LastLogIndex = rf.commitIndex
// 		args.LastLogTerm = 0

// 		reply := RequestVoteReply{}
// 		rf.sendRequestVote(i, &args, &reply)
// 		// client.Call("Raft.RequestVote", &args, &reply)
// 		// 处理返回值 如果获得多数票 则停止
// 		rf.mu.Lock()
// 		log.Printf("raft %d receive vote from %v for term %d reply:%v", rf.me, rf, rf.currentTerm, reply)
// 		// 如果收到多数票 —— 成为 leader；立即发送空 AppendEntries（心跳）以建立权威。
// 		if (reply.VoteGranted) {
// 			log.Printf("raft %d received vote for term %d", rf.me, rf.currentTerm)
// 			voted++
// 		}
// 		rf.mu.Unlock()
// 		// 多数票：大于等于半数节点
// 		if (voted >= majority) {
// 			log.Printf("raft %d 获取到大多数投票，当选leader", rf.me)
// 			break
// 		}
// 	}
// 	if (voted >= majority) {
// 		rf.mu.Lock()
// 		rf.status = Leader
// 		// 停止选举定时器
// 		log.Printf("raft %d 成为 Leader (term=%d)，停止选举定时器", rf.me, rf.currentTerm)
// 		if rf.electionTimer.timer != nil && !rf.electionTimer.timer.Stop() {
// 			select{
// 			case <- rf.electionTimer.timer.C:
// 			default:
// 			}
// 		}
// 		rf.mu.Unlock()
// 		// 立即发送空 AppendEntries（心跳）以建立权威。
// 		go rf.broadcastAppendEntries()
// 	}
// }

// func (rf *Raft) Election() {
// 	rf.mu.Lock()
// 	rf.currentTerm++
// 	rf.votedFor = rf.me
// 	rf.status = Candidate
// 	log.Printf("raft %d 变成候选人开始选举 for term %d", rf.me, rf.currentTerm)
// 	rf.mu.Unlock()
// 	// 给其他节点发送请求投票 RPC
// 	go rf.broadcastVote()
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
// 	rf := &Raft{}
// 	rf.peers = peers
// 	rf.persister = persister
// 	rf.me = me

// 	// Your initialization code here (2A, 2B, 2C).
// 	rf.currentTerm = 0
// 	rf.votedFor = -1
// 	rf.log = make([]string, 100)
// 	rf.log = append(rf.log, "init")
// 	rf.commitIndex = 0
// 	rf.lastApplied = 0
// 	rf.status = Follower
// 	rf.resetCh = make(chan struct{}, 1)

// 	// initialize from state persisted before a crash
// 	rf.readPersist(persister.ReadRaftState())

// 	// go channel
// 	go func() {
// 		// 初始化随机数种子
// 		rand.Seed(time.Now().UnixNano())
// 		// Sleep 随机毫秒数
// 		// 创建一个计时器
// 		// 定义随机超时范围（论文推荐 150-300ms）
// 		minTimeout := 150 * time.Millisecond
// 		maxTimeout := 300 * time.Millisecond

// 		rf.mu.Lock()
// 		timeout := randomTimeout(minTimeout, maxTimeout)
// 		log.Printf("raft %d Sleeping for %v...\n", me, timeout)
// 		timer := time.NewTimer(timeout)
// 		electionTimer := ElectionTimer{}
// 		electionTimer.min = minTimeout
// 		electionTimer.max = maxTimeout
// 		electionTimer.timer = timer
// 		electionTimer.timeout = timeout
// 		rf.electionTimer = electionTimer
// 		rf.mu.Unlock()
// 		for !rf.killed() {
// 			select {
// 			case <- timer.C:
// 				rf.mu.Lock()
// 				if rf.status != Leader {
// 					log.Printf("raft %d 超时，发起选举", me)
// 					// 发起选举
// 					rf.mu.Unlock()
// 					rf.Election()
// 				}
// 				rf.mu.Lock()
// 				// 重新设置 timer（timer.C 已被触发）
// 				timeout = randomTimeout(minTimeout, maxTimeout)
// 				timer.Reset(timeout)
// 				rf.electionTimer.timeout = timeout
// 				rf.mu.Unlock()
// 			case <- rf.resetCh:
// 				// 收到心跳：安全 Stop + Reset timer（在锁下）
// 				rf.mu.Lock()
// 				if !timer.Stop() {
// 					// 若已触发但未被读取，drain
// 					select {
// 					case <-timer.C:
// 					default:
// 					}
// 				}
// 				timeout = randomTimeout(minTimeout, maxTimeout)
// 				timer.Reset(timeout)
// 				rf.electionTimer.timeout = timeout
// 				rf.mu.Unlock()
// 			}
// 		}

// 	}()
// 	log.Printf("raft %d started", me)
// 	return rf
// }

// // 随机生成一个超时时间
// func randomTimeout(min, max time.Duration) time.Duration {
//     delta := max - min
//     return min + time.Duration(rand.Int63n(int64(delta)))
// }

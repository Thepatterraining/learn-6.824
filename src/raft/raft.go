package raft

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"learn-6.824/src/labrpc"
)

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
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftStatus int

const (
	Follower RaftStatus = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
// 实现单个Raft对等体的Go对象
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 需要持久化
	currentTerm int      //当前任期
	votedFor    int      // 投票给谁
	log         []string // log信息

	// 不需要持久化
	commitIndex int // 已提交的最大索引
	lastApplied int // 已应用到状态机的最大索引

	// leader的属性
	nextIndex  []int // 下次要发送给每个follower的日志索引
	matchIndex []int // 对端已复制的最大日志索引

	// 节点状态
	status RaftStatus
	// 心跳时间
	lastHeartBeatTime atomic.Value
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // 候选人最后一条日志的 index（用于判断日志新旧）
	LastLogTerm  int // 候选人最后一条日志的 term。
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
	defer rf.mu.Unlock()
	// 检查是否满足条件 满足就投票
	log.Printf("raft %d received RequestVote from %d for term %d, args:%v,", rf.me, args.CandidateId, args.Term, args)
	// 如果 RPC 的 term < currentTerm，立即回复 false（拒绝），并返回自己的 currentTerm。
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// currentTerm := rf.currentTerm
	if args.Term > rf.currentTerm {
		// 如果 RPC 包含更大的 term，要更新并退化为 follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}
	// // 收到相同 term，但已投给别人 拒绝投票
	// if args.Term == rf.currentTerm && rf.votedFor != -1 {
	// 	log.Printf("raft %d 收到相同 term，但已投给别人 拒绝投票 投票给 %d", rf.me, rf.votedFor)
	// 	reply.VoteGranted = false
	// 	reply.Term = rf.currentTerm
	// 	return
	// }
	// log.Printf("raft %d checking vote for %d for term %d", rf.me, args.CandidateId, args.Term)
	// 否则（term >= currentTerm）需要检查两点：
	// 本节点尚未在该 term 投票（votedFor == -1 或已投给 candidateId）；
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		log.Printf("raft %d 检查日志新旧 for %d for term %d", rf.me, args.CandidateId, args.Term)
		// 候选人的日志至少跟接收者的日志一样新（比较 lastLogTerm，若相同则比较 lastLogIndex）。 todo
		// if args.LastLogTerm == rf.t && args.LastLogIndex >= rf.commitIndex {
		log.Printf("raft %d 投票给 %d for term %d", rf.me, args.CandidateId, args.Term)
		// 满足这两点则授予投票（voteGranted = true），并把 votedFor = candidateId（并在稳定存储上持久化）。
		rf.votedFor = args.CandidateId
		rf.status = Follower
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
		// }
	}

	log.Printf("兜底")
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
	log.Printf("sendAppendEntries server %d args %v reply %v", server, args, reply)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler.
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int // leader’s term
	LeaderId     int
	PrevLogIndex int      // Leader的Log里面的上一个LogIndex
	PrevLogTerm  int      // Leader的Log里面的上一个LogIndex的Term
	Entries      []string // log信息
	LeaderCommit int      // leader’s commitIndex
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int // 接收者的 currentTerm
	Success bool
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
	log.Printf("raft %d 收到心跳 from raft %d for term %d, 当前term: %d args:%v", rf.me, args.LeaderId, args.Term, rf.currentTerm, args)
	// 重置心跳时间
	rf.lastHeartBeatTime.Store(time.Now())
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		log.Printf("raft %d 收到心跳，但是term小于当前term", rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	log.Printf("raft %d 收到心跳 from raft %d for term %d, 33333 当前term: %d ", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.Term > rf.currentTerm {
		// 更新term
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = Follower
	}
	log.Printf("raft %d 收到心跳 from raft %d for term %d, 44444 当前term: %d ", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.Success = true
	log.Printf("raft %d 收到心跳 from raft %d for term %d, end 当前term: %d ", rf.me, args.LeaderId, args.Term, rf.currentTerm)

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 心跳
func (rf *Raft) broadcastAppendEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		// 只有leader才发送心跳
		if rf.status != Leader {
			rf.mu.Unlock()
			return
		}
		// 重置心跳时间
		rf.lastHeartBeatTime.Store(time.Now())
		log.Printf("raft %d 开始发送心跳 term %d", rf.me, rf.currentTerm)
		currentTerm := rf.currentTerm
		me := rf.me
		commitIndex := rf.commitIndex
		rf.mu.Unlock()
		// 发送心跳
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peer int) {
				args := AppendEntriesArgs{}
				args.Term = currentTerm
				args.LeaderId = me
				args.PrevLogIndex = 0
				args.PrevLogTerm = 0
				args.Entries = nil
				args.LeaderCommit = commitIndex
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					//If AppendEntries RPC received from new leader: convert to follower
					if reply.Term > rf.currentTerm {
						log.Printf("raft %d 降级成为follower", rf.me)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.status = Follower
						rf.mu.Unlock()
						return
					}
					// 状态检查
					if currentTerm != rf.currentTerm || rf.status != Leader {
						rf.mu.Unlock()
						return
					}
					log.Printf("raft %d 心跳返回 for raft %d term %d reply:%v", rf.me, peer, rf.currentTerm, reply)
					rf.mu.Unlock()
				}
			}(i)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 广播选举
func (rf *Raft) broadcastVote(me int, currentTerm int, commitIndex int) {
	voted := int32(0) // 自己的1票
	atomic.StoreInt32(&voted, 1)
	majority := (len(rf.peers) + 1) / 2
	log.Printf("raft %d 票数 %d 多数节点 %d", me, voted, majority)
	for i := range rf.peers {
		if i == me {
			continue
		}
		go func(peer int) {
			args := RequestVoteArgs{}
			args.CandidateId = me
			args.Term = currentTerm
			args.LastLogIndex = commitIndex
			args.LastLogTerm = 0

			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				// 处理返回值 如果获得多数票 则停止
				rf.mu.Lock()
				log.Printf("raft %d 处理投票返回 for term %d reply:%v", me, currentTerm, reply)
				// 如果返回的term更大，代表已经进入了下一个term选举
				if reply.Term > currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.status = Follower
					rf.mu.Unlock()
					return
				}
				// 状态检查
				if rf.status != Candidate || rf.currentTerm != currentTerm {
					rf.mu.Unlock()
					return
				}
				// If votes received from majority of servers: become leader
				// 如果收到多数票 —— 成为 leader；立即发送空 AppendEntries（心跳）以建立权威。
				if reply.VoteGranted {
					log.Printf("raft %d received vote for term %d", rf.me, rf.currentTerm)
					// 原子+1
					atomic.AddInt32(&voted, 1)
					log.Printf("raft %d 当前票数 %d", rf.me, atomic.LoadInt32(&voted))
					if atomic.LoadInt32(&voted) >= int32(majority) {
						rf.status = Leader
						// 立即发送空 AppendEntries（心跳）以建立权威。
						rf.mu.Unlock()
						go rf.broadcastAppendEntries()
					} else {
						rf.mu.Unlock()
					}
				} else {
					rf.mu.Unlock()
				}
			}
		}(i)
	}

}

// • Increment currentTerm
// • Vote for self
// • Reset election timer
// • Send RequestVote RPCs to all other servers
func (rf *Raft) Election() {
	rf.mu.Lock()
	if rf.status == Leader {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.status = Candidate
	me := rf.me
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	// 重置选举定时器
	rf.lastHeartBeatTime.Store(time.Now())
	log.Printf("raft %d 变成候选人开始选举 for term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	// 给其他节点发送请求投票 RPC
	rf.broadcastVote(me, currentTerm, commitIndex)
}

// 选举定时器
func (rf *Raft) runElectionTimer() {
	// Sleep 随机毫秒数
	// 创建一个计时器
	// 定义随机超时范围（论文推荐 150-300ms）
	minTimeout := 200 * time.Millisecond
	maxTimeout := 400 * time.Millisecond
	timeout := randomTimeout(minTimeout, maxTimeout)
	for !rf.killed() {
		since := time.Since(rf.lastHeartBeatTime.Load().(time.Time))
		log.Printf("raft %d since %v  Sleeping for timeout %v", rf.me, since, timeout)
		if since > timeout {
			// 超过超时时间
			log.Printf("raft %d 超时选举", rf.me)
			rf.Election()
		}
		time.Sleep(timeout)
	}
}

// 创建 Raft 节点检查日志新旧
func makeRaftNode(peers []*labrpc.ClientEnd, me int, persister *Persister) *Raft {
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
	rf := makeRaftNode(peers, me, persister)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// go channel 选举定时器
	go rf.runElectionTimer()
	log.Printf("raft %d started", me)
	return rf
}

// 随机生成一个超时时间
func randomTimeout(min, max time.Duration) time.Duration {
	delta := max - min
	return min + time.Duration(rand.Int63n(int64(delta)))
}

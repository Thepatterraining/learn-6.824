package raft

// import (
// 	"log"
// 	"math/rand"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	"learn-6.824/src/labrpc"
// )

// // --------------- ApplyMsg ---------------

// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int
// }

// // --------------- 节点状态枚举 ---------------

// type RaftStatus int

// const (
// 	Follower RaftStatus = iota
// 	Candidate
// 	Leader
// )

// // --------------- 主结构体 ---------------

// type Raft struct {
// 	mu        sync.Mutex
// 	peers     []*labrpc.ClientEnd
// 	persister *Persister
// 	me        int
// 	dead      int32

// 	// 持久化状态
// 	currentTerm int
// 	votedFor    int

// 	// 日志暂时不用
// 	log []string

// 	// 临时状态
// 	status      RaftStatus
// 	commitIndex int
// 	lastApplied int

// 	// 心跳与选举
// 	resetCh chan struct{}
// }

// // --------------- 基础状态 ---------------

// func (rf *Raft) GetState() (int, bool) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	return rf.currentTerm, rf.status == Leader
// }

// func (rf *Raft) persist()                {}
// func (rf *Raft) readPersist(data []byte) {}

// // --------------- RPC 定义 ---------------

// type RequestVoteArgs struct {
// 	Term         int
// 	CandidateId  int
// 	LastLogIndex int
// 	LastLogTerm  int
// }

// type RequestVoteReply struct {
// 	Term        int
// 	VoteGranted bool
// }

// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if args.Term < rf.currentTerm {
// 		reply.Term = rf.currentTerm
// 		reply.VoteGranted = false
// 		return
// 	}

// 	// 若 term 更大，更新自己为 follower
// 	if args.Term > rf.currentTerm {
// 		rf.currentTerm = args.Term
// 		rf.votedFor = -1
// 		rf.status = Follower
// 	}

// 	// 若未投票 或 已经投给此 candidate，则投票
// 	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
// 		rf.votedFor = args.CandidateId
// 		rf.status = Follower
// 		reply.VoteGranted = true
// 		reply.Term = rf.currentTerm
// 		// 收到投票请求说明对方活跃，重置超时
// 		select {
// 		case rf.resetCh <- struct{}{}:
// 		default:
// 		}
// 		log.Printf("raft %d 投票给 %d for term %d", rf.me, args.CandidateId, args.Term)
// 		return
// 	}

// 	reply.VoteGranted = false
// 	reply.Term = rf.currentTerm
// }

// type AppendEntriesArgs struct {
// 	Term         int
// 	LeaderId     int
// 	PrevLogIndex int
// 	PrevLogTerm  int
// 	Entries      []string
// 	LeaderCommit int
// }

// type AppendEntriesReply struct {
// 	Term    int
// 	Success bool
// }

// func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if args.Term < rf.currentTerm {
// 		reply.Term = rf.currentTerm
// 		reply.Success = false
// 		return
// 	}

// 	// 如果 term 更大，更新 term 并变 follower
// 	if args.Term > rf.currentTerm {
// 		rf.currentTerm = args.Term
// 		rf.votedFor = -1
// 		rf.status = Follower
// 	}

// 	// 心跳 / AppendEntries 都算 leader 活跃
// 	select {
// 	case rf.resetCh <- struct{}{}:
// 	default:
// 	}

// 	reply.Term = rf.currentTerm
// 	reply.Success = true
// 	log.Printf("raft %d 收到来自 %d 的心跳 term=%d", rf.me, args.LeaderId, args.Term)
// }

// // --------------- RPC 发送函数 ---------------

// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	return rf.peers[server].Call("Raft.RequestVote", args, reply)
// }

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
// }

// // --------------- 选举逻辑 ---------------

// func (rf *Raft)         () {
// 	rf.mu.Lock()
// 	rf.currentTerm++
// 	rf.status = Candidate
// 	rf.votedFor = rf.me
// 	currentTerm := rf.currentTerm
// 	me := rf.me
// 	rf.mu.Unlock()

// 	log.Printf("raft %d 发起选举 term=%d", me, currentTerm)

// 	votes := int32(1)
// 	total := len(rf.peers)

// 	for i := range rf.peers {
// 		if i == me {
// 			continue
// 		}

// 		go func(peer int) {
// 			args := RequestVoteArgs{
// 				Term:         currentTerm,
// 				CandidateId:  me,
// 				LastLogIndex: 0,
// 				LastLogTerm:  0,
// 			}
// 			reply := RequestVoteReply{}

// 			if rf.sendRequestVote(peer, &args, &reply) {
// 				rf.mu.Lock()
// 				defer rf.mu.Unlock()

// 				if reply.Term > rf.currentTerm {
// 					rf.currentTerm = reply.Term
// 					rf.status = Follower
// 					rf.votedFor = -1
// 					return
// 				}

// 				if rf.status != Candidate || currentTerm != rf.currentTerm {
// 					return
// 				}

// 				if reply.VoteGranted {
// 					atomic.AddInt32(&votes, 1)
// 					if atomic.LoadInt32(&votes) > int32(total/2) {
// 						// 赢得选举
// 						rf.status = Leader
// 						log.Printf("raft %d 成为 Leader (term=%d)", me, rf.currentTerm)
// 						go rf.leaderLoop()
// 					}
// 				}
// 			}
// 		}(i)
// 	}
// }

// // --------------- Leader 心跳循环 ---------------

// func (rf *Raft) leaderLoop() {
// 	ticker := time.NewTicker(100 * time.Millisecond)
// 	defer ticker.Stop()

// 	for !rf.killed() {
// 		rf.mu.Lock()
// 		if rf.status != Leader {
// 			rf.mu.Unlock()
// 			return
// 		}
// 		currentTerm := rf.currentTerm
// 		me := rf.me
// 		rf.mu.Unlock()

// 		for i := range rf.peers {
// 			if i == me {
// 				continue
// 			}
// 			go func(peer int) {
// 				args := AppendEntriesArgs{
// 					Term:     currentTerm,
// 					LeaderId: me,
// 				}
// 				reply := AppendEntriesReply{}
// 				rf.sendAppendEntries(peer, &args, &reply)
// 			}(i)
// 		}
// 		<-ticker.C
// 	}
// }

// // --------------- 主循环（选举超时控制） ---------------

// func (rf *Raft) runElectionTimer() {
// 	for !rf.killed() {
// 		timeout := randomTimeout(150*time.Millisecond, 300*time.Millisecond)
// 		timer := time.NewTimer(timeout)

// 		select {
// 		case <-rf.resetCh:
// 			if !timer.Stop() {
// 				select {
// 				case <-timer.C:
// 				default:
// 				}
// 			}
// 			continue // 重置定时器，重新循环
// 		case <-timer.C:
// 			rf.mu.Lock()
// 			if rf.status != Leader {
// 				go rf.startElection()
// 			}
// 			rf.mu.Unlock()
// 		}
// 	}
// }

// // --------------- 工具函数 ---------------

// func randomTimeout(min, max time.Duration) time.Duration {
// 	return min + time.Duration(rand.Int63n(int64(max-min)))
// }

// // --------------- 外部接口 ---------------

// func (rf *Raft) Start(command interface{}) (int, int, bool) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	return -1, rf.currentTerm, rf.status == Leader
// }

// func (rf *Raft) Kill() {
// 	atomic.StoreInt32(&rf.dead, 1)
// }

// func (rf *Raft) killed() bool {
// 	return atomic.LoadInt32(&rf.dead) == 1
// }

// // --------------- 构造函数 ---------------

// func Make(peers []*labrpc.ClientEnd, me int,
// 	persister *Persister, applyCh chan ApplyMsg) *Raft {

// 	rf := &Raft{
// 		peers:     peers,
// 		persister: persister,
// 		me:        me,
// 		votedFor:  -1,
// 		status:    Follower,
// 		resetCh:   make(chan struct{}, 10),
// 	}

// 	// 恢复持久化状态
// 	rf.readPersist(persister.ReadRaftState())

// 	go rf.runElectionTimer()
// 	log.Printf("raft %d started", me)
// 	return rf
// }

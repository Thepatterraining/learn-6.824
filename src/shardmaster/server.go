package shardmaster

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"learn-6.824/src/labgob"
	"learn-6.824/src/labrpc"
	"learn-6.824/src/raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs       []Config // indexed by config num
	nextConfigNum int32
	pendingOps    map[int]chan struct{}
	pendingCmds   map[int]Op
}

type Op struct {
	// Your data here.
	Servers map[int][]string
	Option  string
	SeqNum  int32
	Shards  [NShards]int
	Gids    []int
}

func loadBalance(config *Config) {
	// 简单的负载均衡算法：轮询分配分片
	gids := make([]int, 0)
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	numGroups := len(gids)
	sort.Ints(gids)
	DPrintf("load Balance gids:%v", gids)
	if numGroups == 0 {
		// 没有组，所有分片归0
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		return
	}
	for i := 0; i < NShards; i++ {
		config.Shards[i] = gids[i%numGroups]
	}
	return
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	seqNum := atomic.LoadInt32(&sm.nextConfigNum)
	atomic.AddInt32(&sm.nextConfigNum, 1)
	sm.mu.Unlock()
	command := Op{
		Servers: args.Servers,
		Option:  "join",
		SeqNum:  seqNum,
	}
	ok := sm.RequestRaft(command)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
	return
}

func (sm *ShardMaster) executeJoin(command Op) {
	servers := make(map[int][]string)
	sm.mu.Lock()
	config := sm.getLastConfig()
	sm.mu.Unlock()
	for gid := range config.Groups {
		// 复制已有的组
		servers[gid] = make([]string, len(config.Groups[gid]))
		copy(servers[gid], config.Groups[gid])
	}
	DPrintf("join 旧组：%v", servers)
	for gid, srvList := range command.Servers {
		servers[gid] = srvList
	}
	DPrintf("join 新组：%v", servers)

	newConfig := Config{
		Num:    int(command.SeqNum),
		Shards: [NShards]int{},
		Groups: servers,
	}

	// 负载均衡地分配分片
	loadBalance(&newConfig)
	DPrintf("[Node:%d] shardmaster server execute join new config:%v", sm.me, newConfig)
	// 添加新配置
	sm.mu.Lock()
	sm.configs = append(sm.configs, newConfig)
	DPrintf("[Node:%d] shardmaster server execute join 所有配置:%v", sm.me, sm.configs)
	sm.mu.Unlock()

}

func (sm *ShardMaster) RequestRaft(command Op) bool {
	for {
		DPrintf("[Node:%d] shardmaster server raft op:%s, servers:%v", sm.me, command.Option, command.Servers)
		index, _, isLeader := sm.rf.Start(command)
		if !isLeader {
			DPrintf("[Node:%d] shardmaster server raft not leader", sm.me)
			return false
		}
		sm.mu.Lock()
		sm.pendingCmds[index] = command
		doneCh := make(chan struct{})
		sm.pendingOps[index] = doneCh
		sm.mu.Unlock()
		// 等待
		select {
		case <-doneCh:
			DPrintf("[Node:%d] shardmaster server raft repley: success op:%s, servers:%v", sm.me, command.Option, command.Servers)
			sm.mu.Lock()
			delete(sm.pendingOps, index)
			sm.mu.Unlock()
			return true
		case <-time.After(1000 * time.Millisecond):
			DPrintf("[Node:%d] shardmaster server raft timeout:%s op:%s, servers:%v", sm.me, "timeout", command.Option, command.Servers)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (sm *ShardMaster) listenApplyCh() {
	for {
		applyMsg := <-sm.applyCh
		DPrintf("[Node:%d] shardmaster server listenApplyCh applyMsg:%v data:%v", sm.me, applyMsg, sm.configs)
		if !applyMsg.IsSnapshot && applyMsg.CommandValid {
			// 防止nil Command被传递到上层 - 检查Command是否为nil
			if applyMsg.Command == nil {
				DPrintf("[Node:%d] shardmaster server listenApplyCh received nil command at index:%d, skipping",
					sm.me, applyMsg.CommandIndex)
				continue
			}

			// 使用安全的类型断言
			op, ok := applyMsg.Command.(Op)
			if !ok {
				DPrintf("[Node:%d] shardmaster server listenApplyCh command type assertion failed at index:%d, skipping", sm.me, applyMsg.CommandIndex)
				continue
			}
			// sm.mu.Lock()
			// 幂等行检查
			// seqNum, exists := sm.seqNums[op.ClientId]
			// if op.Option != "Get" {
			// 	if exists && op.SeqNum <= seqNum {
			// 		DPrintf("[Node:%d] kv server listenApplyCh op:%s, key:%s, value:%s 重复操作", kv.serverId, op.Option, op.Key, op.Value)
			// 		// 通知返回成功
			// 		doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
			// 		if exists {
			// 			close(doneCh)
			// 			delete(kv.pendingOps, applyMsg.CommandIndex)
			// 		}
			// 		kv.mu.Unlock()
			// 		continue
			// 	}
			// }
			// if !exists || op.SeqNum > seqNum {
			// 	// 更新最新的SeqNum
			// 	DPrintf("[Node:%d] kv server listenApplyCh op:%s, key:%s, value:%s clientid:%d, 更新seqNum:%d", kv.serverId, op.Option, op.Key, op.Value, op.ClientId, op.SeqNum)
			// 	kv.seqNums[op.ClientId] = op.SeqNum
			// }
			switch op.Option {
			case "join":
				sm.executeJoin(op)
				// 检查Put/Append操作
				// 通知返回成功
				sm.mu.Lock()
				doneCh, exists := sm.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := sm.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if sm.opEquals(originalCmd, op) {
						close(doneCh)
						delete(sm.pendingOps, applyMsg.CommandIndex)
					}
				}
				sm.mu.Unlock()
			case "query":
				// 通知返回成功
				sm.mu.Lock()
				doneCh, exists := sm.pendingOps[applyMsg.CommandIndex]
				DPrintf("[Node:%d] shardmaster server listen query: exists:%v", sm.me, exists)
				if exists {
					originalCmd := sm.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if sm.opEquals(originalCmd, op) {
						close(doneCh)
						delete(sm.pendingOps, applyMsg.CommandIndex)
					}
				}
				sm.mu.Unlock()
			case "leave":
				sm.executeLeave(op)
				// 通知返回成功
				sm.mu.Lock()
				doneCh, exists := sm.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := sm.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if sm.opEquals(originalCmd, op) {
						close(doneCh)
						delete(sm.pendingOps, applyMsg.CommandIndex)
					}
				}
				sm.mu.Unlock()
			case "move":
				sm.executeMove(op)
				// 通知返回成功
				sm.mu.Lock()
				doneCh, exists := sm.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := sm.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if sm.opEquals(originalCmd, op) {
						close(doneCh)
						delete(sm.pendingOps, applyMsg.CommandIndex)
					}
				}
				sm.mu.Unlock()
			}
			// sm.mu.Unlock()
		}
	}
}

// 辅助方法：比较两个Op是否相等
func (sm *ShardMaster) opEquals(op1, op2 Op) bool {
	return op1.SeqNum == op2.SeqNum &&
		op1.Option == op2.Option
}

func (sm *ShardMaster) executeLeave(command Op) {
	gids := make([]int, 0)
	newGroups := make(map[int][]string)
	sm.mu.Lock()
	config := sm.getLastConfig()
	sm.mu.Unlock()
	for gid := range config.Groups {
		gids = append(gids, gid)
		newGroups[gid] = make([]string, len(config.Groups[gid]))
		copy(newGroups[gid], config.Groups[gid])
	}
	// 删除指定gids
	deleteGids := command.Gids
	for _, gid := range deleteGids {
		delete(newGroups, gid)
	}

	newConfig := Config{
		Num:    int(command.SeqNum),
		Shards: [NShards]int{},
		Groups: newGroups,
	}
	// 负载均衡地分配分片
	loadBalance(&newConfig)
	DPrintf("[Node:%d] shardmaster server execute leave new config:%v", sm.me, newConfig)
	// 添加新配置
	sm.mu.Lock()
	sm.configs = append(sm.configs, newConfig)
	DPrintf("[Node:%d] shardmaster server execute leave 所有配置:%v", sm.me, sm.configs)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// 取出最新的gids
	DPrintf("[Node:%d] shardmaster server leave param:%v", sm.me, args.GIDs)
	sm.mu.Lock()
	seqNum := atomic.LoadInt32(&sm.nextConfigNum)
	atomic.AddInt32(&sm.nextConfigNum, 1)
	sm.mu.Unlock()
	command := Op{
		Option: "leave",
		SeqNum: seqNum,
		Gids:   args.GIDs,
	}
	ok := sm.RequestRaft(command)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
	return
}

func (sm *ShardMaster) executeMove(command Op) {
	newConfig := Config{
		Num:    int(command.SeqNum),
		Shards: command.Shards,
		Groups: command.Servers,
	}
	atomic.AddInt32(&sm.nextConfigNum, 1)

	// 添加新配置
	sm.mu.Lock()
	sm.configs = append(sm.configs, newConfig)
	DPrintf("[Node:%d] shardmaster server execute move 所有配置:%v", sm.me, sm.configs)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	config := sm.getLastConfig()
	sm.mu.Unlock()
	newShards := [NShards]int{}
	for i := 0; i < NShards; i++ {
		newShards[i] = config.Shards[i]
	}
	newShards[args.Shard] = args.GID
	newGroups := make(map[int][]string)
	for gid := range config.Groups {
		newGroups[gid] = make([]string, len(config.Groups[gid]))
		copy(newGroups[gid], config.Groups[gid])
	}

	command := Op{
		Option:  "move",
		SeqNum:  atomic.LoadInt32(&sm.nextConfigNum),
		Shards:  newShards,
		Servers: newGroups,
	}
	ok := sm.RequestRaft(command)
	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false
	reply.Err = OK
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("[Node:%d] shardmaster server query param:%d", sm.me, args.Num)
	maxNum := len(sm.configs)
	command := Op{
		Option: "query",
		SeqNum: int32(args.Num),
	}
	ok := sm.RequestRaft(command)
	if !ok {
		reply.WrongLeader = true
		return
	}
	DPrintf("[Node:%d] shardmaster server query maxNum:%d, current max num:%d", sm.me, maxNum, atomic.LoadInt32(&sm.nextConfigNum))
	if args.Num == -1 || args.Num >= maxNum {
		// 返回最新配置
		sm.mu.Lock()
		reply.Config = sm.getLastConfig()
		sm.mu.Unlock()
		DPrintf("当前所有配置:%v 返回最新配置:%v", sm.configs, reply.Config)
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
	// 返回对应配置
	sm.mu.Lock()
	reply.Config = sm.configs[args.Num]
	sm.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK
	return
}

func (sm *ShardMaster) getLastConfig() Config {
	maxNum := len(sm.configs)
	return sm.configs[maxNum-1]
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	atomic.StoreInt32(&sm.nextConfigNum, 1)
	sm.pendingOps = make(map[int]chan struct{})
	sm.pendingCmds = make(map[int]Op)

	go sm.listenApplyCh()
	return sm
}

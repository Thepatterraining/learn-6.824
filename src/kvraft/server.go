package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"learn-6.824/src/labgob"
	"learn-6.824/src/labrpc"
	"learn-6.824/src/raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	Option string
	SeqNum string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string

	rfSem       chan struct{}
	pendingOps  map[int]chan struct{}
	getOps      map[int]chan struct{}
	seqNums     map[string]bool
	pendingCmds map[int]Op
	serverId    string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		Key:    args.Key,
		Value:  "",
		Option: "Get",
		SeqNum: args.SeqNum,
	}
	DPrintf("[Node:%s] kv server Get key:%s", kv.serverId, args.Key)
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrNotLeader
		DPrintf("[Node:%s] kv server Get repley:%s", kv.serverId, reply.Err)
		return
	}
	kv.mu.Lock()
	doneCh := make(chan struct{})
	kv.getOps[index] = doneCh
	kv.mu.Unlock()
	// 等待
	select {
	case <-doneCh:
		reply.Err = "success"
		kv.mu.Lock()
		value, exists := kv.data[args.Key]
		if !exists {
			value = ""
		}
		reply.Value = value
		DPrintf("[Node:%s] kv server get key:%s, value:%s", kv.serverId, args.Key, value)
		kv.mu.Unlock()
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("[Node:%s] kv server get key:%s, timeout", kv.serverId, args.Key)
	}
	kv.mu.Lock()
	delete(kv.getOps, index)
	kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 检查SeqNum是否已存在
	kv.mu.Lock()
	_, exists := kv.seqNums[args.SeqNum]
	kv.mu.Unlock()
	if exists {
		// 重复操作，直接返回成功
		reply.Err = "success"
		DPrintf("[Node:%s] kv server putAppend op:%s, key:%s, value:%s 重复操作", kv.serverId, args.Op, args.Key, args.Value)
		return
	}
	command := Op{
		Key:    args.Key,
		Value:  args.Value,
		Option: args.Op,
		SeqNum: args.SeqNum,
	}
	DPrintf("[Node:%s] kv server putAppend op:%s, key:%s, value:%s", kv.serverId, args.Op, args.Key, args.Value)
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrNotLeader
		DPrintf("[Node:%s] kv server putAppend repley:%s", kv.serverId, reply.Err)
		return
	}
	kv.mu.Lock()
	kv.pendingCmds[index] = command
	doneCh := make(chan struct{})
	kv.pendingOps[index] = doneCh
	kv.mu.Unlock()
	// 等待
	select {
	case <-doneCh:
		reply.Err = "success"
		DPrintf("[Node:%s] kv server putAppend repley: success op:%s, key:%s, value:%s", kv.serverId, args.Op, args.Key, args.Value)
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("[Node:%s] kv server putAppend timeout:%s op:%s, key:%s, value:%s", kv.serverId, reply.Err, args.Op, args.Key, args.Value)
	}
	kv.mu.Lock()
	delete(kv.pendingOps, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) listenApplyCh() {
	for {
		applyMsg := <-kv.applyCh
		DPrintf("[Node:%s] kv server listenApplyCh applyMsg:%v", kv.serverId, applyMsg)
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			// 幂等行检查
			if op.Option != "Get" {
				_, exists := kv.seqNums[op.SeqNum]
				if exists {
					DPrintf("[Node:%s] kv server listenApplyCh op:%s, key:%s, value:%s 重复操作", kv.serverId, op.Option, op.Key, op.Value)
					// 通知返回成功
					doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
					if exists {
						close(doneCh)
						delete(kv.pendingOps, applyMsg.CommandIndex)
					}
					kv.mu.Unlock()
					continue
				}
			}
			kv.seqNums[op.SeqNum] = true
			switch op.Option {
			case "Put":
				kv.put(op.Key, op.Value)
				// 检查Put/Append操作
				// 通知返回成功
				doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := kv.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if kv.opEquals(originalCmd, op) {
						close(doneCh)
						delete(kv.pendingOps, applyMsg.CommandIndex)
					}
				}
			case "Append":
				kv.append(op.Key, op.Value)
				// 检查Put/Append操作
				// 通知返回成功
				doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := kv.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if kv.opEquals(originalCmd, op) {
						close(doneCh)
						delete(kv.pendingOps, applyMsg.CommandIndex)
					}
				}
			case "Get":
				// 通知返回成功
				doneCh, exists := kv.getOps[applyMsg.CommandIndex]
				if exists {
					close(doneCh)
					delete(kv.getOps, applyMsg.CommandIndex)
				}
			}
			kv.mu.Unlock()
		}
	}
}

// 辅助方法：通知等待者并验证命令
// func (kv *KVServer) notifyWaiters(index int, appliedOp Op, isDuplicate bool) {
// 	// 检查Get操作
// 	if getCh, exists := kv.getOps[index]; exists {
// 		originalCmd := kv.getCmds[index]
// 		// 验证命令是否匹配
// 		if kv.opEquals(originalCmd, appliedOp) {
// 			getCh <- OpResult{Success: true, Value: kv.data[appliedOp.Key]}
// 		} else {
// 			getCh <- OpResult{Success: false}
// 		}
// 		delete(kv.getOps, index)
// 		delete(kv.getCmds, index)
// 	}

// 	// 检查Put/Append操作
// 	// 通知返回成功
// 	doneCh, exists := kv.pendingOps[index]
// 	if exists {
// 		originalCmd := kv.pendingCmds[index]
// 		// 验证命令是否匹配
// 		if kv.opEquals(originalCmd, appliedOp) {
// 			close(doneCh)
// 			delete(kv.pendingOps, index)
// 		}
// 	}
// }

// 辅助方法：比较两个Op是否相等
func (kv *KVServer) opEquals(op1, op2 Op) bool {
	return op1.Key == op2.Key &&
		op1.Value == op2.Value &&
		op1.Option == op2.Option &&
		op1.SeqNum == op2.SeqNum
}

func (kv *KVServer) put(key, value string) {
	// Implementation for Put operation
	kv.data[key] = value
	DPrintf("[Node:%s] kv server put success key:%s value:%s", kv.serverId, key, value)
}

func (kv *KVServer) append(key, value string) {
	// Implementation for Append operation
	kv.data[key] = kv.data[key] + value
	DPrintf("[Node:%s] kv server append success key:%s value:%s", kv.serverId, key, value)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.data = make(map[string]string)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rfSem = make(chan struct{})
	kv.pendingOps = make(map[int]chan struct{})
	kv.getOps = make(map[int]chan struct{})
	kv.seqNums = make(map[string]bool)
	kv.pendingCmds = make(map[int]Op)
	kv.serverId = GenerateUUID()
	// You may need initialization code here.
	go kv.listenApplyCh()
	return kv
}

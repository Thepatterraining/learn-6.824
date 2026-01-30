package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"learn-6.824/src/labgob"
	"learn-6.824/src/labrpc"
	"learn-6.824/src/raft"
)

const Debug = 0

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
	Key      string
	Value    string
	Option   string
	SeqNum   int64
	ClientId int64
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

	rfSem          chan struct{}
	pendingOps     map[int]chan struct{}
	getOps         map[int]chan struct{}
	seqNums        map[int64]int64
	appliedSeqNums map[int64]int64
	pendingCmds    map[int]Op
	serverId       int64
	persister      *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		Key:      args.Key,
		Value:    "",
		Option:   "Get",
		SeqNum:   args.SeqNum,
		ClientId: args.ClientId,
	}
	DPrintf("[Node:%d] kv server Get key:%s", kv.serverId, args.Key)
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrNotLeader
		DPrintf("[Node:%d] kv server Get repley:%s", kv.serverId, reply.Err)
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
		DPrintf("[Node:%d] kv server get key:%s, data:%v", kv.serverId, args.Key, kv.data)
		value, exists := kv.data[args.Key]
		if !exists {
			value = ""
		}
		reply.Value = value
		DPrintf("[Node:%d] kv server get key:%s, value:%s", kv.serverId, args.Key, value)
		kv.mu.Unlock()
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("[Node:%d] kv server get key:%s, timeout", kv.serverId, args.Key)
	}
	kv.mu.Lock()
	delete(kv.getOps, index)
	kv.mu.Unlock()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 检查SeqNum是否已存在
	kv.mu.Lock()
	seqNum, exists := kv.seqNums[args.ClientId]
	kv.mu.Unlock()
	if exists && args.SeqNum <= seqNum {
		// 重复操作，直接返回成功
		reply.Err = "success"
		DPrintf("[Node:%d] kv server putAppend op:%s, key:%s, value:%s 重复操作", kv.serverId, args.Op, args.Key, args.Value)
		return
	}
	command := Op{
		Key:      args.Key,
		Value:    args.Value,
		Option:   args.Op,
		SeqNum:   args.SeqNum,
		ClientId: args.ClientId,
	}
	DPrintf("[Node:%d] kv server putAppend op:%s, key:%s, value:%s", kv.serverId, args.Op, args.Key, args.Value)
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrNotLeader
		DPrintf("[Node:%d] kv server putAppend repley:%s", kv.serverId, reply.Err)
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
		DPrintf("[Node:%d] kv server putAppend repley: success op:%s, key:%s, value:%s", kv.serverId, args.Op, args.Key, args.Value)
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("[Node:%d] kv server putAppend timeout:%s op:%s, key:%s, value:%s", kv.serverId, reply.Err, args.Op, args.Key, args.Value)
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
		// DPrintf("[Node:%s] kv server listenApplyCh applyMsg:%v data:%v", kv.serverId, applyMsg, kv.data)
		if !applyMsg.IsSnapshot && applyMsg.CommandValid {
			// 防止nil Command被传递到上层 - 检查Command是否为nil
			if applyMsg.Command == nil {
				DPrintf("[Node:%d] kv server listenApplyCh received nil command at index:%d, skipping",
					kv.serverId, applyMsg.CommandIndex)
				continue
			}

			// 使用安全的类型断言
			op, ok := applyMsg.Command.(Op)
			if !ok {
				DPrintf("[Node:%d] kv server listenApplyCh command type assertion failed at index:%d, skipping",
					kv.serverId, applyMsg.CommandIndex)
				continue
			}
			kv.mu.Lock()
			// 幂等行检查
			seqNum, exists := kv.seqNums[op.ClientId]
			if op.Option != "Get" {
				if exists && op.SeqNum <= seqNum {
					DPrintf("[Node:%d] kv server listenApplyCh op:%s, key:%s, value:%s 重复操作", kv.serverId, op.Option, op.Key, op.Value)
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
			if !exists || op.SeqNum > seqNum {
				// 更新最新的SeqNum
				DPrintf("[Node:%d] kv server listenApplyCh op:%s, key:%s, value:%s clientid:%d, 更新seqNum:%d", kv.serverId, op.Option, op.Key, op.Value, op.ClientId, op.SeqNum)
				kv.seqNums[op.ClientId] = op.SeqNum
			}
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
		} else if applyMsg.IsSnapshot {
			// 检查 snapshot 是否比当前状态更新
			// 注意：这里需要 KVServer 跟踪 lastApplied
			// if applyMsg.CommandIndex <= kv.lastAppliedIndex {
			// 	DPrintf("[Node:%d] kv server 忽略过时的snapshot lastIncludedIndex:%d <= lastApplied:%d",
			// 		kv.serverId, applyMsg.CommandIndex, kv.lastAppliedIndex)
			// 	kv.mu.Unlock()
			// 	continue
			// }
			kv.mu.Lock()
			// 恢复快照数据
			data := applyMsg.Snapshot
			kv.data = make(map[string]string)
			for k, v := range data {
				kv.data[k] = v
			}
			// 恢复seqNums，这对幂等性检测至关重要
			// 不要创建新的map，直接替换现有map以避免并发问题
			tempSeqNums := applyMsg.SeqNums
			for k, v := range tempSeqNums {
				kv.seqNums[k] = v
			}

			// 注意：快照恢复时不清理pending操作
			// pending操作会通过正常的幂等性检查自然处理
			// 清理操作可能导致channel被错误关闭，造成duplicate element错误
			DPrintf("[Node:%d] kv server listenApplyCh load snapshot data:%v seqNums:%v", kv.serverId, kv.data, kv.seqNums)
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
	DPrintf("[Node:%d] kv server put success key:%s value:%s data:%v", kv.serverId, key, value, kv.data)
}

func (kv *KVServer) append(key, value string) {
	// Implementation for Append operation
	kv.data[key] = kv.data[key] + value
	DPrintf("[Node:%d] kv server append success key:%s value:%s data:%v", kv.serverId, key, value, kv.data)
}

func (kv *KVServer) generateSnapshotter() {
	for {
		term, isLeader := kv.rf.GetState()
		DPrintf("[Node:%d] kv server generateSnapshotter raft state size: %d, maxraftstate: %d", kv.serverId, kv.persister.RaftStateSize(), kv.maxraftstate)
		if isLeader && kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
			// 触发快照存储
			kv.mu.Lock()
			data := make(map[string]string)
			for k, v := range kv.data {
				data[k] = v
			}
			seqNums := make(map[int64]int64)
			for k, v := range kv.seqNums {
				seqNums[k] = v
			}
			kv.mu.Unlock()
			DPrintf("[Node:%d] kv server generateSnapshotter start data:%v", kv.serverId, data)
			kv.rf.CreateSnapshot(data, kv.rf.GetApplied(), term, seqNums)
		}
		time.Sleep(100 * time.Millisecond)
	}
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
	// 如果有快照，从快照恢复数据
	if persister.SnapshotSize() > 0 {
		kv.restoreSnapshot(persister.ReadSnapshot())
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rfSem = make(chan struct{})
	kv.pendingOps = make(map[int]chan struct{})
	kv.getOps = make(map[int]chan struct{})
	kv.seqNums = make(map[int64]int64)
	kv.appliedSeqNums = make(map[int64]int64)
	kv.pendingCmds = make(map[int]Op)
	kv.serverId = nrand()
	kv.persister = persister
	go kv.listenApplyCh()
	go kv.generateSnapshotter()
	return kv
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	// Example:
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	var snapshotData map[string]string
	var seqNums map[int64]int64
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&snapshotData) != nil ||
		d.Decode(&seqNums) != nil {
		// error
		panic("kv server Failed to read persisted snapshot")
	} else {
		data := make(map[string]string)
		for k, v := range snapshotData {
			data[k] = v
		}
		kv.data = data
		seqNum := make(map[int64]int64)
		for k, v := range seqNums {
			seqNum[k] = v
		}
		kv.seqNums = seqNum
		DPrintf("[Node:%d] kv server restoreSnapshot lastIncludedTerm:%d lastIncludedIndex:%d snapshotData:%v", kv.serverId, lastIncludedTerm, lastIncludedIndex, snapshotData)
		// DPrintf("[Node:%d] kv server restoreSnapshot InstallSnapshot 通知上层KV server data:%v", kv.serverId, snapshotData)
		// Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	}
}

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

	rfSem      chan struct{}
	pendingOps map[int]chan struct{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
	DPrintf("[Node:%d] kv server get key:%s, value:%s", kv.me, args.Key, reply.Value)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{
		Key:    args.Key,
		Value:  args.Value,
		Option: args.Op,
	}
	DPrintf("[Node:%d] kv server putAppend op:%s, key:%s, value:%s", kv.me, args.Op, args.Key, args.Value)
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrNotLeader
		DPrintf("[Node:%d] kv server putAppend repley:%s", kv.me, reply.Err)
		return
	}
	kv.mu.Lock()
	doneCh := make(chan struct{})
	kv.pendingOps[index] = doneCh
	kv.mu.Unlock()
	// 等待
	select {
	case <-doneCh:
		reply.Err = "success"
		DPrintf("[Node:%d] kv server putAppend repley:%s", kv.me, reply.Err)
	case <-time.After(10000 * time.Millisecond):
		reply.Err = ErrTimeout
		DPrintf("[Node:%d] kv server putAppend timeout:%s", kv.me, reply.Err)
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
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			switch op.Option {
			case "Put":
				kv.put(op.Key, op.Value)
			case "Append":
				kv.append(op.Key, op.Value)
			}
			// 通知返回成功
			doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
			if exists {
				close(doneCh)
				delete(kv.pendingOps, applyMsg.CommandIndex)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) put(key, value string) {
	// Implementation for Put operation
	kv.data[key] = value
	DPrintf("[Node:%d] kv server put success key:%s value:%s", kv.me, key, value)
}

func (kv *KVServer) append(key, value string) {
	// Implementation for Append operation
	kv.data[key] = kv.data[key] + value
	DPrintf("[Node:%d] kv server append success key:%s value:%s", kv.me, key, value)
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
	// You may need initialization code here.
	go kv.listenApplyCh()
	return kv
}

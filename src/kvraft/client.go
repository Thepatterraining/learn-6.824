package kvraft

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"learn-6.824/src/labrpc"
)

func GenerateUUID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	isLeader bool
	clientId string
	opId     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	// You'll have to add code here.
	ck.clientId = GenerateUUID()
	ck.opId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:    key,
		SeqNum: fmt.Sprintf("%s-%d", ck.clientId, nrand()),
	}
	tries := 0
	for {
		reply := GetReply{}
		DPrintf("[client:%s] server:%d kv client get key:%s", ck.clientId, ck.leader, key)
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("[client:%s] server:%d kv client get key network error:%t", ck.clientId, ck.leader, ok)
			// 请求其他Server
			ck.updateLeader()
			time.Sleep(10 * time.Millisecond)
			tries = 0
			continue
		}
		if reply.Err == ErrTimeout {
			// 超时，直接重试
			DPrintf("[client:%s] server:%d kv client get key timeout:%t", ck.clientId, ck.leader, ok)
			time.Sleep(10 * time.Millisecond)
			tries++
			if tries >= 3 {
				// 超过重试次数，换leader
				ck.updateLeader()
				tries = 0
			}
			continue
		}
		// You will have to modify this function.
		if reply.Err == ErrNotLeader {
			// 请求其他Server
			DPrintf("[client:%s] server:%d kv client get key repeat reply:%s", ck.clientId, ck.leader, reply.Err)
			ck.updateLeader()
			tries = 0
			continue
		}
		if reply.Err == "success" {
			return reply.Value
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		SeqNum: fmt.Sprintf("%s-%d", ck.clientId, nrand()),
	}
	tries := 0
	timeoutCount := 0
	for tries < 6 {
		reply := PutAppendReply{}
		DPrintf("[client:%s] server:%d kv client putAppend key:%s, value:%s", ck.clientId, ck.leader, key, value)
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			// RPC失败，直接重试
			DPrintf("[client:%s] server:%d kv client putAppend key network error:%t", ck.clientId, ck.leader, ok)
			// 请求其他Server
			time.Sleep(10 * time.Millisecond)
			ck.updateLeader()
			tries = 0
			continue
		}
		if reply.Err == ErrTimeout {
			// 超时，直接重试
			DPrintf("[client:%s] server:%d kv client putAppend key:%s,value:%s timeout:%t", ck.clientId, ck.leader, key, value, ok)
			time.Sleep(1000 * time.Millisecond)
			timeoutCount++
			if timeoutCount >= 3 {
				// 超过重试次数，换leader
				ck.updateLeader()
				tries++
				timeoutCount = 0
			}
			continue
		}
		if reply.Err == ErrNotLeader {
			DPrintf("[client:%s] server:%d kv client putAppend key repeat reply:%s", ck.clientId, ck.leader, reply.Err)
			// 请求其他Server
			ck.updateLeader()
			tries = 0
			continue
		}
		if reply.Err == "success" {
			return
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) updateLeader() {
	DPrintf("[client:%s] 更新leader before:%d, after:%d", ck.clientId, ck.leader, (ck.leader+1)%len(ck.servers))
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

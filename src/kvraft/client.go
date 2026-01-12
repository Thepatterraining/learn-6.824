package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	"learn-6.824/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	isLeader bool
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
		Key: key,
	}
	reply := GetReply{}
	log.Printf("server:%d kv client get key:%s", ck.leader, key)
	ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
	// You will have to modify this function.
	return reply.Value
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
		Key:   key,
		Value: value,
		Op:    op,
	}
	for {
		reply := PutAppendReply{}
		log.Printf("server:%d kv client putAppend key:%s, value:%s", ck.leader, key, value)
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == "success" {
			return
		}
		if !ok || reply.Err == ErrNotLeader {
			// 请求其他Server
			ck.updateLeader()
			log.Printf("server:%d kv client putAppend key repeat reply:%s", ck.leader, reply.Err)
			continue
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
	log.Printf("更新leader before:%d, after:%d", ck.leader, (ck.leader+1)%len(ck.servers))
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

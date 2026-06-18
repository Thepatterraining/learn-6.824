package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"learn-6.824/src/labrpc"
	"learn-6.824/src/shardmaster"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int64
	opId         int64
	shard2Leader map[int]int
}

// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end

	// You'll have to add code here.
	ck.clientId = nrand()
	ck.opId = 0
	ck.shard2Leader = make(map[int]int)
	// ck.config = ck.sm.Query(-1)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	atomic.AddInt64(&ck.opId, 1)
	args := GetArgs{
		Key:      key,
		SeqNum:   atomic.LoadInt64(&ck.opId),
		ClientId: ck.clientId,
	}

	for {

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				// for {
				leader, leaderExists := ck.shard2Leader[shard]
				if !leaderExists {
					leader = 0
					ck.shard2Leader[shard] = 0
				}
				srv := ck.make_end(servers[leader])
				var reply GetReply
				DPrintf("[client:%d] shardkv client get key:%s to server:%v, shard:%d, gid:%d servers:%s config:%v", ck.clientId, key, srv, shard, gid, servers[leader], ck.config)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongLeader) {
					// 这个不是Leader 换下一个server
					// 请求其他Server
					DPrintf("[client:%d] server:%d shardkv client get key repeat reply:%s", ck.clientId, leader, reply.Err)
					ck.updateLeader(shard)
					continue
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				if ok && (reply.Err == ErrWrongStatus) {
					time.Sleep(10 * time.Millisecond)
					break
				}
				if ok && (reply.Err == ErrNoKey) {
					time.Sleep(10 * time.Millisecond)
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok {
					DPrintf("[client:%d] server:%d shardkv client get key call failed", ck.clientId, leader)
					ck.updateLeader(shard)
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op Option) {
	// args := PutAppendArgs{}
	// args.Key = key
	// args.Value = value
	// args.Op = op
	atomic.AddInt64(&ck.opId, 1)
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		SeqNum:   atomic.LoadInt64(&ck.opId),
		ClientId: ck.clientId,
	}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				// for {
				leader, leaderExists := ck.shard2Leader[shard]
				if !leaderExists {
					leader = 0
					ck.shard2Leader[shard] = 0
				}
				srv := ck.make_end(servers[leader])
				var reply PutAppendReply
				DPrintf("[client:%d] shardkv client putappend key:%s value:%s op:%s to server:%v, shard:%d, gid:%d servers:%s", ck.clientId, key, value, op, srv, shard, gid, servers[leader])
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && (reply.Err == ErrWrongLeader) {
					// 这个不是Leader 换下一个server
					DPrintf("[client:%d] server:%d shardkv client putappend key repeat reply:%s", ck.clientId, leader, reply.Err)
					ck.updateLeader(shard)
					continue
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}

				if ok && (reply.Err == ErrWrongStatus) {
					time.Sleep(10 * time.Millisecond)
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok {
					DPrintf("[client:%d] server:%d shardkv client putappend key call failed", ck.clientId, leader)
					ck.updateLeader(shard)
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) updateLeader(shard int) {
	DPrintf("[client:%d] shardkv client 更新leader before:%d, after:%d", ck.clientId, ck.shard2Leader[shard], (ck.shard2Leader[shard]+1)%len(ck.config.Groups[ck.config.Shards[shard]]))
	ck.shard2Leader[shard] = (ck.shard2Leader[shard] + 1) % len(ck.config.Groups[ck.config.Shards[shard]])
}

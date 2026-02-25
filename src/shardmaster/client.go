package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"learn-6.824/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
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
	// Your code here.
	ck.clientId = nrand()
	ck.opId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	atomic.AddInt64(&ck.opId, 1)
	args := &QueryArgs{
		Num:    num,
		SeqNum: atomic.LoadInt64(&ck.opId),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	atomic.AddInt64(&ck.opId, 1)
	args := &JoinArgs{
		Servers: servers,
		SeqNum:  atomic.LoadInt64(&ck.opId),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	atomic.AddInt64(&ck.opId, 1)
	args := &LeaveArgs{
		GIDs:   gids,
		SeqNum: atomic.LoadInt64(&ck.opId),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	atomic.AddInt64(&ck.opId, 1)
	args := &MoveArgs{
		Shard:  shard,
		GID:    gid,
		SeqNum: atomic.LoadInt64(&ck.opId),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

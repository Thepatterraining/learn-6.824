package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Option string

const (
	GET           Option = "GET"
	PUT           Option = "PUT"
	APPEND        Option = "APPEND"
	MIGRATION_IN  Option = "MIGRATION_IN"
	CHANGE_CONFIG Option = "CHANGE_CONFIG"
)

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrWrongStatus      = "ErrWrongStatus"
	ErrWrongConfigCange = "ErrWrongConfigCange"
)

type Err string

type ShardData struct {
	Data   map[string]string
	SeqNum map[int64]int64
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    Option // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqNum   int64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SeqNum   int64
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Put or Append
type MigrationArgs struct {
	// You'll have to add definitions here.
	Data map[string]string
	// shard id
	Shard int
	// config num
	ConfigNum int
	SeqNum    map[int64]int64
	ShardData map[int]ShardData // shardid => shard data
	ClientId  int64
}

type MigrationReply struct {
	Err Err
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

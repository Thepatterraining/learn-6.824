package shardkv

// import "../shardmaster"
import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"learn-6.824/src/labgob"
	"learn-6.824/src/labrpc"
	"learn-6.824/src/raft"
	"learn-6.824/src/shardmaster"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key           string
	Value         string
	Option        Option
	ShardId       int
	SeqNum        int64
	ClientId      int64
	Config        shardmaster.Config
	MigrationData map[int]ShardData
	ConfigNum     int
	// MigrationSeqNum map[int64]int64
}

type Shard struct {
	shardStatus raft.ShardKVStatus // 状态
	data        map[string]string  // key -> value
	seqNums     map[int64]int64
	mu          sync.Mutex
	ownerGid    int
	configNum   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string // key -> value
	shard2Key   map[int][]string  // shardId -> key list
	pendingOps  map[int]chan Err
	pendingCmds map[int]Op
	status      raft.ShardKVStatus
	dead        int32 // set by Kill()
	persister   *raft.Persister
	seqNums     map[int64]int64
	shardInfo   map[int]*Shard // shard id -> shard info
	isLeader    bool

	// 配置
	lastConfigNum int
	config        shardmaster.Config
	lastConfig    shardmaster.Config
	currentConfig shardmaster.Config
	// 负责的分片
	shards map[int]bool // shardId -> bool

}

func (kv *ShardKV) checkGroup(key string) bool {
	shard := key2shard(key)
	kv.lock("checkGroup")
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server checkGroup key:%s, shard:%d shards:%v", kv.me, kv.gid, kv.isLeader, key, shard, kv.shards)
	shardInfo := kv.shardInfo[shard]
	isShardOwner := shardInfo.ownerGid == kv.gid
	kv.unlock("checkGroup")
	return isShardOwner
}

func (kv *ShardKV) checkParam(key string) Err {
	// 检查是否负责这个分片
	shard := key2shard(key)
	kv.lock("checkParam")
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server checkGroup key:%s, shard:%d shards:%v", kv.me, kv.gid, kv.isLeader, key, shard, kv.shards)
	isLeader := kv.isLeader
	shardInfo := kv.shardInfo[shard]
	status := shardInfo.shardStatus
	isShardOwner := shardInfo.ownerGid == kv.gid
	kv.unlock("checkParam")
	// 检查是否leader
	if !isLeader {
		return ErrWrongLeader
	}
	if !isShardOwner {
		// 不负责这个分片了，返回错误
		return ErrWrongGroup
	}
	if status != raft.Normal {
		return ErrWrongStatus
	}
	return OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 检查是否负责这个分片
	err := kv.checkParam(args.Key)
	if err != OK {
		DPrintf("[Node:%d GID:%d] shardkv server get key:%s, check param failed Err:%s", kv.me, kv.gid, args.Key, err)
		reply.Err = err
		return
	}
	shard := key2shard(args.Key)
	command := Op{
		Key:      args.Key,
		Value:    "",
		Option:   GET,
		SeqNum:   args.SeqNum,
		ClientId: args.ClientId,
		ShardId:  shard,
	}
	err = kv.RequestRaft(command)
	if err != OK {
		DPrintf("[Node:%d GID:%d] shardkv server get key:%s, check param failed Err:%s", kv.me, kv.gid, args.Key, err)
		reply.Err = err
		return
	}
	kv.lock("Get")
	shardInfo := kv.shardInfo[shard]
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server get key:%s, data:%v", kv.me, kv.gid, kv.isLeader, args.Key, shardInfo.data)
	value, exists := shardInfo.data[args.Key]
	if !exists {
		DPrintf("[Node:%d GID:%d] shardkv server get key:%s, check param failed Err:%s", kv.me, kv.gid, args.Key, ErrNoKey)
		reply.Err = ErrNoKey
		return
	}
	reply.Value = value
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server get key:%s, value:%s", kv.me, kv.gid, kv.isLeader, args.Key, value)
	kv.unlock("Get")
	reply.Err = OK
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 检查SeqNum是否已存在
	shard := key2shard(args.Key)
	kv.lock("put append seq num check")
	shardInfo := kv.shardInfo[shard]
	seqNum, exists := shardInfo.seqNums[args.ClientId]
	kv.unlock("put append seq num check")
	if exists && args.SeqNum <= seqNum {
		// 重复操作，直接返回成功
		reply.Err = "success"
		DPrintf("[Node:%d GID:%d] kv server putAppend op:%s, key:%s, value:%s 重复操作", kv.me, kv.gid, args.Op, args.Key, args.Value)
		return
	}

	err := kv.checkParam(args.Key)
	if err != OK {
		DPrintf("[Node:%d GID:%d] shardkv server putAppend key:%s, check param failed Err:%s", kv.me, kv.gid, args.Key, err)
		reply.Err = err
		return
	}
	command := Op{
		Key:      args.Key,
		Value:    args.Value,
		Option:   args.Op,
		SeqNum:   args.SeqNum,
		ClientId: args.ClientId,
		ShardId:  shard,
	}
	err = kv.RequestRaft(command)
	if err != OK {
		DPrintf("[Node:%d GID:%d] shardkv server putAppend key:%s, check param failed Err:%s", kv.me, kv.gid, args.Key, err)
		reply.Err = err
		return
	}

	reply.Err = OK
	return
}

func (kv *ShardKV) Migration(args *MigrationArgs, reply *MigrationReply) {
	// Your code here.
	// 检查SeqNum是否已存在
	// kv.lock("put append seq num check")
	// seqNum, exists := kv.seqNums[args.ClientId]
	// kv.unlock("put append seq num check")
	// if exists && args.SeqNum <= seqNum {
	// 	// 重复操作，直接返回成功
	// 	reply.Err = "success"
	// 	DPrintf("[Node:%d GID:%d] kv server putAppend op:Migration, seqnum:%d, clientId:%d Data:%v 重复操作", kv.me, kv.gid, args.SeqNum, args.ClientId, args.Data)
	// 	return
	// }
	// 检查是否leader
	kv.lock("check param")
	// status := kv.status
	isLeader := kv.isLeader
	configNum := kv.config.Num
	kv.unlock("check param")
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server migration args:%v", kv.me, kv.gid, isLeader, args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum > configNum {
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server migration wrong args config num:%v current config num:%d", kv.me, kv.gid, isLeader, args.ConfigNum, configNum)
		reply.Err = ErrWrongConfigCange
		return
	}
	// kv.lock("Migration")
	// kv.status = Migration
	// isLeader := kv.isLeader
	// kv.unlock("Migration")
	command := Op{
		MigrationData: args.ShardData,
		// MigrationSeqNum: args.SeqNum,
		Option: MIGRATION_IN,
		// ClientId:  args.ClientId,
		ConfigNum: args.ConfigNum,
	}
	err := kv.RequestRaft(command)
	if err != OK {
		DPrintf("[Node:%d GID:%d] shardkv server migration request raft failed Err:%s", kv.me, kv.gid, err)
		reply.Err = err
		kv.lock("migration error")
		kv.status = raft.Normal
		kv.unlock("migration error")
		return
	}
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server migration success", kv.me, kv.gid, isLeader)
	reply.Err = OK
	return
}

func (kv *ShardKV) RequestRaft(command Op) Err {
	for {
		index, _, isLeader := kv.rf.Start(command)
		kv.lock("RequestRaft update isLeader")
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server raft command:%v", kv.me, kv.gid, isLeader, command)
		kv.isLeader = isLeader
		kv.unlock("RequestRaft update isLeader")
		if !isLeader {
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server raft not leader", kv.me, kv.gid, isLeader)
			return OK
		}
		doneCh := make(chan Err)
		kv.lock("RequestRaft add pendingOps")
		kv.pendingCmds[index] = command
		kv.pendingOps[index] = doneCh
		kv.unlock("RequestRaft add pendingOps")
		// 等待
		select {
		case err := <-doneCh:
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server raft repley: success op:%s, key:%s value:%s", kv.me, kv.gid, isLeader, command.Option, command.Key, command.Value)
			kv.lock("RequestRaft delete pendingOps")
			delete(kv.pendingOps, index)
			kv.unlock("RequestRaft delete pendingOps")
			return err
		case <-time.After(1000 * time.Millisecond):
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server raft timeout:%s op:%s, key:%s value:%s", kv.me, kv.gid, isLeader, "timeout", command.Option, command.Key, command.Value)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) diffConfig(oldConfig, newConfig shardmaster.Config) bool {
	// 首次diffConfig，直接打印配置变更日志，不进行迁移
	kv.lock("diffConfig")
	isLeader := kv.isLeader
	defer kv.unlock("diffConfig")
	// if oldConfig.Num == 0 {
	// 	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server diffConfig old config num:%d, new config num:%d, old config:%v, new config:%v", kv.me, kv.gid, isLeader, oldConfig.Num, newConfig.Num, oldConfig, newConfig)
	// 	return true
	// }
	// 真正diff
	if oldConfig.Num != newConfig.Num && oldConfig.Num < newConfig.Num && kv.diffShard(oldConfig.Shards, newConfig.Shards) {
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server diffConfig old config num:%d, new config num:%d, old config:%v, new config:%v", kv.me, kv.gid, isLeader, oldConfig.Num, newConfig.Num, oldConfig, newConfig)
		// 这里可以添加迁移逻辑，例如将数据迁移到新的分片服务器
		return true
	}
	return false
}

func (kv *ShardKV) diffShard(oldShards, newShards [shardmaster.NShards]int) bool {
	isDiff := false
	for i := 0; i < shardmaster.NShards; i++ {
		if oldShards[i] != newShards[i] {
			isDiff = true
			// 这个分片需要迁移
			shardInfo := kv.shardInfo[i]
			shardInfo.shardStatus = raft.MigrationOut
		}
	}
	return isDiff
}

func (kv *ShardKV) hasMigration(shardInfo map[int]*Shard) bool {
	kv.lock("hasMigration")
	defer kv.unlock("hasMigration")
	for _, info := range kv.shardInfo {
		if info.shardStatus == raft.MigrationOut || info.shardStatus == raft.MigrationIn {
			return true
		}
	}
	return false
}

/**
* 监听配置变更的goroutine
* 1. 定期查询ShardMaster的配置
* 2. 如果配置发生变更，更新本地配置并进行相应的迁移操作
 */
func (kv *ShardKV) listenConfigChange() {
	// 定期查询ShardMaster的配置
	args := &shardmaster.QueryArgs{}
	for !kv.killed() {
		kv.lock("listenConfigChange")
		config := kv.config
		isLeader := kv.isLeader
		lastConfigNum := kv.config.Num
		shardInfo := kv.shardInfo
		kv.unlock("listenConfigChange")
		if !isLeader || kv.hasMigration(shardInfo) {
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenConfigChange 不是leader或者有等待迁移的数据", kv.me, kv.gid, isLeader)
			_, isLeader := kv.rf.GetState()
			kv.lock("listenConfigChange2")
			kv.isLeader = isLeader
			kv.unlock("listenConfigChange2")
			time.Sleep(90 * time.Millisecond)
			continue
		}
		args.Num = lastConfigNum + 1
		// try each known server.
		for _, srv := range kv.masters {
			var reply shardmaster.QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				if reply.Config.Num > lastConfigNum {
					DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenConfigChange new config num:%d, old config num:%d args:%v", kv.me, kv.gid, isLeader, reply.Config.Num, lastConfigNum, args)
					// 这里可以添加迁移逻辑，例如将数据迁移到新的分片服务器
					if kv.diffConfig(config, reply.Config) {
						// 发送diff指令给Raft，等待Raft应用到状态机后进行迁移
						cmd := Op{
							Option: CHANGE_CONFIG,
							Config: reply.Config,
							SeqNum: int64(reply.Config.Num),
						}
						DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenConfigChange new config num:%d, old config num:%d cmd:%v", kv.me, kv.gid, isLeader, reply.Config.Num, lastConfigNum, cmd)
						_, _, isLeader := kv.rf.Start(cmd)
						kv.lock("listenConfigChange update isLeader")
						kv.isLeader = isLeader
						kv.unlock("listenConfigChange update isLeader")
					}
				}
			}
		}
		time.Sleep(90 * time.Millisecond)
	}
}

func (kv *ShardKV) executeConfigChange(newConfig shardmaster.Config) {
	// 这里可以添加具体的配置变更处理逻辑
	// 开始迁移数据
	// _, isLeader := kv.rf.GetState()
	kv.lock("executeConfigChange migration")
	if newConfig.Num <= kv.config.Num {
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server executeConfigChange new config num:%d is not greater than old config num:%d", kv.me, kv.gid, kv.isLeader, newConfig.Num, kv.config.Num)
		kv.unlock("executeConfigChange migration")
		return
	}
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server executeConfigChange new config num:%d, old config num:%d", kv.me, kv.gid, kv.isLeader, newConfig.Num, kv.config.Num)
	isLeader := kv.isLeader
	gid2Data := make(map[int]map[int]ShardData)
	for i := 0; i < shardmaster.NShards; i++ {
		shardInfo := kv.shardInfo[i]
		newGid := newConfig.Shards[i]
		oldGid := kv.config.Shards[i]
		if newGid == kv.gid {
			// 这个分片我负责了
			shardInfo.ownerGid = newGid
			shardInfo.configNum = newConfig.Num
			if oldGid == 0 || oldGid == kv.gid {
				shardInfo.shardStatus = raft.Normal
			} else {
				shardInfo.shardStatus = raft.MigrationIn
			}
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server executeConfigChange migrate shard:%d from %d to %d, keyList:%v shard status:%s", kv.me, kv.gid, isLeader, i, oldGid, newGid, gid2Data, shardInfo.shardStatus)
			continue
		}
		// 这个分片我不负责
		shardInfo.ownerGid = newGid
		shardInfo.shardStatus = raft.Invalid
		shardInfo.configNum = newConfig.Num
		if oldGid == kv.gid {
			// 这个分片有数据要迁移出去
			shardInfo.shardStatus = raft.MigrationOut
			// 1. 检查外层 map 是否已经为该 newGid 创建了子 map
			if _, ok := gid2Data[newGid]; !ok {
				gid2Data[newGid] = make(map[int]ShardData)
			}
			// 2. 将当前 shard (i) 的数据存入该 gid 对应的子 map 中
			shardData := ShardData{}
			if len(shardInfo.data) > 0 {
				shardData = ShardData{
					Data:   shardInfo.data, // 假设这里需要深拷贝，看你的具体需求
					SeqNum: shardInfo.seqNums,
				}
			}
			gid2Data[newGid][i] = shardData
		}
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server executeConfigChange migrate shard:%d from %d to %d, keyList:%v shard status:%s", kv.me, kv.gid, isLeader, i, oldGid, newGid, gid2Data[newGid], shardInfo.shardStatus)
	}
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server executeConfigChange migrate gid2keyList:%v", kv.me, kv.gid, isLeader, gid2Data)
	// 开始迁移数据
	// kv.mu.Lock()
	// kv.status = Migration
	kv.config = newConfig
	kv.unlock("executeConfigChange migration")
	// kv.unlock("executeConfigChange migration leader")
	if len(gid2Data) > 0 && isLeader {
		kv.requestMigration(newConfig, gid2Data)
	}

	// 迁移完成后更新配置
	kv.lock("executeConfigChange migration leader update config")
	for i := 0; i < shardmaster.NShards; i++ {
		shardInfo := kv.shardInfo[i]
		// 迁移出去的状态更新成无效
		if shardInfo.shardStatus == raft.MigrationOut {
			shardInfo.shardStatus = raft.Invalid
		}
	}
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server executeConfigChange 更新负责的分片:%v", kv.me, kv.gid, kv.isLeader, kv.shardInfo)
	kv.unlock("executeConfigChange migration leader update config")

}

func (kv *ShardKV) sendMigration(servers []string, args MigrationArgs, isLeader bool, i int, newGid int) {
	for !kv.killed() {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply MigrationReply
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv client migration data:%v to server:%v, shard:%d, gid:%d server:%v confignum:%d", kv.me, kv.gid, isLeader, args.ShardData, srv, i, newGid, servers[si], args.ConfigNum)
			ok := srv.Call("ShardKV.Migration", &args, &reply)
			if ok && reply.Err == OK {
				DPrintf("[Node:%d GID:%d isLeader:%v] shardkv client migration data:%v to server:%v, shard:%d, gid:%d server:%v succeess", kv.me, kv.gid, isLeader, args.ShardData, srv, i, newGid, servers[si])
				return
			}
			if ok && (reply.Err == ErrWrongGroup) {
				DPrintf("[Node:%d GID:%d isLeader:%v] shardkv client migration data:%v to server:%v, shard:%d, gid:%d server:%v ErrWrongGroup", kv.me, kv.gid, isLeader, args.ShardData, srv, i, newGid, servers[si])
				time.Sleep(10 * time.Millisecond)
				break
			}
			if ok && (reply.Err == ErrWrongLeader) {
				// 这个不是Leader 换下一个server
				DPrintf("[Node:%d GID:%d isLeader:%v] shardkv client migration data:%v to server:%v, shard:%d, gid:%d server:%v ErrWrongLeader", kv.me, kv.gid, isLeader, args.ShardData, srv, i, newGid, servers[si])
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if ok && (reply.Err == ErrWrongConfigCange) {
				DPrintf("[Node:%d GID:%d isLeader:%v] shardkv client migration data:%v to server:%v, shard:%d, gid:%d server:%v ErrWrongConfigCange", kv.me, kv.gid, isLeader, args.ShardData, srv, i, newGid, servers[si])
				time.Sleep(10 * time.Millisecond)
				break
			}
			// ... not ok, or ErrWrongLeader
		}
	}
}

func (kv *ShardKV) requestMigration(newConfig shardmaster.Config, gid2Data map[int]map[int]ShardData) {
	for newGid, migrationData := range gid2Data {
		i := 0
		if kv.gid == newGid {
			continue
		}
		kv.lock("requestMigration")
		// 复制一份数据迁移出去
		shard2Data := make(map[int]ShardData)
		for shard, data := range migrationData {
			argsData := make(map[string]string)
			for k, v := range data.Data {
				argsData[k] = v
			}
			argsSeqnum := make(map[int64]int64)
			for k, v := range data.SeqNum {
				argsSeqnum[k] = v
			}
			shard2Data[shard] = ShardData{
				Data:   argsData,
				SeqNum: argsSeqnum,
			}
		}

		args := MigrationArgs{
			// Data:      argsData,
			// Shard:     i,
			ConfigNum: newConfig.Num,
			// SeqNum:    argsSeqnum,
			// ClientId:  int64(maxClientId),
			ShardData: shard2Data,
		}
		isLeader := kv.isLeader
		kv.unlock("requestMigration")
		servers, ok := newConfig.Groups[newGid]
		if ok {
			go kv.sendMigration(servers, args, isLeader, i, newGid)
		}

	}

}

func (kv *ShardKV) migrateData(migrationData map[int]ShardData, configNum int) {
	for shard, shardData := range migrationData {
		// kv.data[key] = value
		// shard := key2shard(key)
		shardInfo := kv.shardInfo[shard]
		if shardInfo.shardStatus == raft.MigrationIn {
			for k, v := range shardData.Data {
				shardInfo.data[k] = v
			}
			for k, v := range shardData.SeqNum {
				shardInfo.seqNums[k] = v
			}
			shardInfo.configNum = configNum
			shardInfo.ownerGid = kv.gid
			shardInfo.shardStatus = raft.Normal
		}
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server executeConfigChange migrate shard:%d status:%s", kv.me, kv.gid, kv.isLeader, shard, shardInfo.shardStatus)
	}

	// // 将 migration in 更新成 normal
	// for i := 0; i < shardmaster.NShards; i++ {
	// 	shardInfo := kv.shardInfo[i]
	// 	if shardInfo.shardStatus == MigrationIn {
	// 		shardInfo.shardStatus = Normal
	// 		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server executeConfigChange migrate shard status:%s", kv.me, kv.gid, kv.isLeader, shardInfo.shardStatus)

	// 	}
	// }
	// kv.status = Normal
}

func (kv *ShardKV) listenApplyCh() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.lock("listen applych")
		isLeader := kv.isLeader
		kv.unlock("listen applych")
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenApplyCh applyMsg:%v", kv.me, kv.gid, isLeader, applyMsg)
		if applyMsg.CommandValid {
			// 防止nil Command被传递到上层 - 检查Command是否为nil
			if applyMsg.Command == nil {
				DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenApplyCh received nil command at index:%d, skipping",
					kv.me, kv.gid, isLeader, applyMsg.CommandIndex)
				continue
			}

			// 使用安全的类型断言
			op, ok := applyMsg.Command.(Op)
			if !ok {
				DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenApplyCh command type assertion failed at index:%d, skipping",
					kv.me, kv.gid, isLeader, applyMsg.CommandIndex)
				continue
			}

			// 幂等行检查
			kv.lock("listen applych seqnums check")
			shardInfo := kv.shardInfo[op.ShardId]
			seqNum, exists := shardInfo.seqNums[op.ClientId]
			kv.unlock("listen applych seqnums check")
			if op.Option != GET && op.Option != CHANGE_CONFIG && op.Option != MIGRATION_IN {
				if exists && op.SeqNum <= seqNum {
					DPrintf("[Node:%d GID:%d isLeader:%v] kv server listenApplyCh op:%s, key:%s, value:%s 重复操作", kv.me, kv.gid, isLeader, op.Option, op.Key, op.Value)
					// 通知返回成功
					kv.lock("listen applych seqnums check 2")
					doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
					if exists {
						doneCh <- OK
						close(doneCh)
						delete(kv.pendingOps, applyMsg.CommandIndex)
					}
					kv.unlock("listen applych seqnums check 2")
					continue
				}
			}
			if !exists || op.SeqNum > seqNum {
				// 更新最新的SeqNum
				DPrintf("[Node:%d GID:%d isLeader:%v] kv server listenApplyCh op:%s, key:%s, value:%s clientid:%d, 更新seqNum:%d", kv.me, kv.gid, isLeader, op.Option, op.Key, op.Value, op.ClientId, op.SeqNum)
				kv.lock("listen applych seqnums check update")
				shardInfo.seqNums[op.ClientId] = op.SeqNum
				kv.unlock("listen applych seqnums check update")
			}
			switch op.Option {
			case PUT:
				kv.lock("listen applych put")
				kv.put(op.Key, op.Value, op.ShardId)
				// 检查Put/Append操作
				// 通知返回成功
				doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := kv.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if kv.opEquals(originalCmd, op) {
						// 再次判断是否是自己负责的分片
						shardInfo := kv.shardInfo[op.ShardId]
						if shardInfo.ownerGid == kv.gid {
							doneCh <- OK
						} else {
							doneCh <- ErrWrongGroup
						}
						close(doneCh)
						delete(kv.pendingOps, applyMsg.CommandIndex)
					}
				}
				kv.unlock("listen applych put")
			case APPEND:
				kv.lock("listen applych append")
				kv.append(op.Key, op.Value, op.ShardId)
				// 检查Put/Append操作
				// 通知返回成功
				doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := kv.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if kv.opEquals(originalCmd, op) {
						// 再次判断是否是自己负责的分片
						shardInfo := kv.shardInfo[op.ShardId]
						if shardInfo.ownerGid == kv.gid {
							doneCh <- OK
						} else {
							doneCh <- ErrWrongGroup
						}
						close(doneCh)
						delete(kv.pendingOps, applyMsg.CommandIndex)
					}
				}
				kv.unlock("listen applych append")
			case GET:
				kv.lock("listen applych get")
				// 通知返回成功
				// doneCh, exists := kv.getOps[applyMsg.CommandIndex]
				// if exists {
				// 	close(doneCh)
				// 	delete(kv.getOps, applyMsg.CommandIndex)
				// }
				// 通知返回成功
				doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := kv.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if kv.opEquals(originalCmd, op) {
						// 再次判断是否是自己负责的分片
						shardInfo := kv.shardInfo[op.ShardId]
						if shardInfo.ownerGid == kv.gid {
							doneCh <- OK
						} else {
							doneCh <- ErrWrongGroup
						}
						close(doneCh)
						delete(kv.pendingOps, applyMsg.CommandIndex)
					}
				}
				kv.unlock("listen applych get")
			case CHANGE_CONFIG:
				// 处理配置变更
				DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenApplyCh apply config num:%d", kv.me, kv.gid, isLeader, op.Config.Num)
				kv.executeConfigChange(op.Config)
			case MIGRATION_IN:
				DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenApplyCh apply migration data:%v", kv.me, kv.gid, isLeader, op.MigrationData)
				kv.lock("listen applych migration")
				kv.migrateData(op.MigrationData, op.ConfigNum)
				// 通知返回成功
				doneCh, exists := kv.pendingOps[applyMsg.CommandIndex]
				if exists {
					originalCmd := kv.pendingCmds[applyMsg.CommandIndex]
					// 验证命令是否匹配
					if kv.opEquals(originalCmd, op) {
						doneCh <- OK
						close(doneCh)
						delete(kv.pendingOps, applyMsg.CommandIndex)
					}
				}
				kv.unlock("listen applych migration")
			}

		} else if applyMsg.IsSnapshot {
			// 检查 snapshot 是否比当前状态更新
			args := &shardmaster.QueryArgs{}
			args.Num = applyMsg.ConfigNum
			config := kv.config
			for _, srv := range kv.masters {
				var reply shardmaster.QueryReply
				ok := srv.Call("ShardMaster.Query", args, &reply)
				if ok && reply.WrongLeader == false {
					config = reply.Config
					break
				}
			}
			kv.lock("listen applych snapshot start")
			// 恢复快照数据
			data := applyMsg.Shard2Data
			kv.data = make(map[string]string)
			for shard, shardData := range data {
				shardInfo := kv.shardInfo[shard]
				for k, v := range shardData {
					shardInfo.data[k] = v
				}
			}
			// 恢复seqNums，这对幂等性检测至关重要
			// 不要创建新的map，直接替换现有map以避免并发问题
			tempSeqNums := applyMsg.Shard2SeqNums
			for shard, seqNums := range tempSeqNums {
				shardInfo := kv.shardInfo[shard]
				for k, v := range seqNums {
					shardInfo.seqNums[k] = v
				}
			}
			for shard, status := range applyMsg.Shard2Status {
				shardInfo := kv.shardInfo[shard]
				shardInfo.shardStatus = status
			}
			// 恢复gid
			for shard, gid := range config.Shards {
				shardInfo := kv.shardInfo[shard]
				shardInfo.ownerGid = gid
			}
			kv.config = config
			// 注意：快照恢复时不清理pending操作
			// pending操作会通过正常的幂等性检查自然处理
			// 清理操作可能导致channel被错误关闭，造成duplicate element错误
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server listenApplyCh load snapshot data:%v seqNums:%v config:%v", kv.me, kv.gid, isLeader, data, tempSeqNums, config)
			kv.unlock("listen applych snapshot start")
		}
	}
}

// 辅助方法：比较两个Op是否相等
func (kv *ShardKV) opEquals(op1, op2 Op) bool {
	return op1.Key == op2.Key &&
		op1.Value == op2.Value &&
		op1.Option == op2.Option
}

func (kv *ShardKV) put(key, value string, shard int) {
	// Implementation for Put operation
	shardInfo := kv.shardInfo[shard]
	shardInfo.data[key] = value
	// kv.data[key] = value
	// kv.shard2Key[key2shard(key)] = append(kv.shard2Key[key2shard(key)], key)
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server put success key:%s value:%s data:%v", kv.me, kv.gid, kv.isLeader, key, value, shardInfo.data)
}

func (kv *ShardKV) append(key, value string, shard int) {
	shardInfo := kv.shardInfo[shard]
	shardInfo.data[key] = shardInfo.data[key] + value
	// Implementation for Append operation
	// kv.data[key] = kv.data[key] + value
	DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server append success key:%s value:%s data:%v", kv.me, kv.gid, kv.isLeader, key, value, shardInfo.data)
}

func (kv *ShardKV) lock(key string) {
	DPrintf("[Node:%d GID:%d] shardkv server lock key:%s", kv.me, kv.gid, key)
	kv.mu.Lock()
}

func (kv *ShardKV) unlock(key string) {
	DPrintf("[Node:%d GID:%d] shardkv server unlock key:%s", kv.me, kv.gid, key)
	kv.mu.Unlock()
}

func (kv *ShardKV) generateSnapshotter() {
	for !kv.killed() {
		// 触发快照存储
		_, isLeader := kv.rf.GetState()
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server generateSnapshotter raft state size: %d, maxraftstate: %d", kv.me, kv.gid, isLeader, kv.persister.RaftStateSize(), kv.maxraftstate)
		kv.lock("generate snapshot")
		data := make(map[int]map[string]string)
		seqNums := make(map[int]map[int64]int64)
		shard2Status := make(map[int]raft.ShardKVStatus)
		for shard, kvData := range kv.shardInfo {
			data[shard] = make(map[string]string)
			for k, v := range kvData.data {
				data[shard][k] = v
			}
			seqNums[shard] = make(map[int64]int64)
			for k, v := range kvData.seqNums {
				seqNums[shard][k] = v
			}
			shard2Status[shard] = kvData.shardStatus
		}
		kv.isLeader = isLeader
		snapshot := raft.Snapshot{
			Shard2Data:    data,
			Shard2SeqNums: seqNums,
			Shard2Status:  shard2Status,
			ConfigNum:     kv.config.Num,
		}
		kv.unlock("generate snapshot")
		if isLeader && kv.persister.RaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server generateSnapshotter start data:%v", kv.me, kv.gid, isLeader, data)
			kv.rf.CreateShardSnapshot2(snapshot, kv.rf.GetApplied())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.pendingOps = make(map[int]chan Err)
	kv.pendingCmds = make(map[int]Op)
	kv.data = make(map[string]string)
	kv.shard2Key = make(map[int][]string)
	kv.seqNums = make(map[int64]int64)
	kv.status = raft.Normal
	kv.persister = persister
	kv.lastConfigNum = 0
	shards := [10]int{0}
	kv.config = shardmaster.Config{Num: 0, Shards: shards}
	kv.shards = make(map[int]bool)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shards[i] = true
	}
	kv.shardInfo = make(map[int]*Shard)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardInfo[i] = &Shard{
			shardStatus: raft.Invalid,
			data:        make(map[string]string),
			seqNums:     make(map[int64]int64),
			ownerGid:    0,
			configNum:   0,
		}
	}
	// 如果有快照，从快照恢复数据
	if persister.SnapshotSize() > 0 {
		kv.restoreSnapshot(persister.ReadSnapshot())
	}

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	_, kv.isLeader = kv.rf.GetState()
	go kv.listenApplyCh()
	go kv.listenConfigChange()
	go kv.generateSnapshotter()
	DPrintf("[Node:%d GID:%d] shardkv server start...", kv.me, kv.gid)
	return kv
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
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
	var shard2Data map[int]map[string]string
	var shard2SeqNums map[int]map[int64]int64
	var shard2Status map[int]raft.ShardKVStatus
	var configNum int
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&snapshotData) != nil ||
		d.Decode(&seqNums) != nil ||
		d.Decode(&shard2Data) != nil ||
		d.Decode(&shard2SeqNums) != nil ||
		d.Decode(&shard2Status) != nil ||
		d.Decode(&configNum) != nil {
		// error
		panic("shardkv server Failed to read persisted snapshot")
	} else {
		// try each known server.
		args := &shardmaster.QueryArgs{}
		args.Num = configNum
		config := kv.config
		for _, srv := range kv.masters {
			var reply shardmaster.QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				config = reply.Config
				break
			}
		}

		kv.lock("resotre snapshot")
		for shard, shardData := range shard2Data {
			shardInfo := kv.shardInfo[shard]
			for k, v := range shardData {
				shardInfo.data[k] = v
			}
		}
		// 恢复seqNums，这对幂等性检测至关重要
		// 不要创建新的map，直接替换现有map以避免并发问题
		for shard, seqNums := range shard2SeqNums {
			shardInfo := kv.shardInfo[shard]
			for k, v := range seqNums {
				shardInfo.seqNums[k] = v
			}
		}

		for shard, status := range shard2Status {
			shardInfo := kv.shardInfo[shard]
			shardInfo.shardStatus = status
		}

		// 恢复gid
		for shard, gid := range config.Shards {
			shardInfo := kv.shardInfo[shard]
			shardInfo.ownerGid = gid
		}
		for shard, shardInfo := range kv.shardInfo {
			DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server restoreSnapshot shardInfo shard:%d ownerGid:%d status:%s data:%v configNum:%d", kv.me, kv.gid, kv.isLeader, shard, shardInfo.ownerGid, shardInfo.shardStatus, shardInfo.data, shardInfo.configNum)
		}
		isLeader := kv.isLeader
		kv.config = config
		kv.unlock("resotre snapshot")
		DPrintf("[Node:%d GID:%d isLeader:%v] shardkv server restoreSnapshot lastIncludedTerm:%d lastIncludedIndex:%d snapshotData:%v config:%v", kv.me, kv.gid, isLeader, lastIncludedTerm, lastIncludedIndex, snapshotData, config)
	}
}

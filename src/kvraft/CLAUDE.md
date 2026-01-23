
## 任务

As things stand now with your code, a rebooting server replays the complete Raft log in order to restore its state. However, it's not practical for a long-running server to remember the complete Raft log forever. Instead, you'll modify Raft and kvserver to cooperate to save space: from time to time kvserver will persistently store a "snapshot" of its current state, and Raft will discard log entries that precede the snapshot. When a server restarts (or falls far behind the leader and must catch up), the server first installs a snapshot and then replays log entries from after the point at which the snapshot was created. Section 7 of the extended Raft paper outlines the scheme; you will have to design the details.

You must design an interface between your Raft library and your service that allows your Raft library to discard log entries. You must revise your Raft code to operate while storing only the tail of the log. Raft should discard old log entries in a way that allows the Go garbage collector to free and re-use the memory; this requires that there be no reachable references (pointers) to the discarded log entries.

The tester passes maxraftstate to your StartKVServer(). maxraftstate indicates the maximum allowed size of your persistent Raft state in bytes (including the log, but not including snapshots). You should compare maxraftstate to persister.RaftStateSize(). Whenever your key/value server detects that the Raft state size is approaching this threshold, it should save a snapshot, and tell the Raft library that it has snapshotted, so that Raft can discard old log entries. If maxraftstate is -1, you do not have to snapshot. maxraftstate applies to the GOB-encoded bytes your Raft passes to persister.SaveRaftState().

### task

1. Modify your Raft so that it can be given a log index, discard the entries before that index, and continue operating while storing only log entries after that index. Make sure all the Lab 2 Raft tests still succeed.
2. Modify your kvserver so that it detects when the persisted Raft state grows too large, and then hands a snapshot to Raft and tells Raft that it can discard old log entries. Raft should save each snapshot with persister.SaveStateAndSnapshot() (don't use files). A kvserver instance should restore the snapshot from the persister when it re-starts.
3. Modify your Raft leader code to send an InstallSnapshot RPC to a follower when the leader has discarded log entries that the follower needs. When a follower receives an InstallSnapshot RPC, it must hand the included snapshot to its kvserver. You can use the applyCh for this purpose, by adding new fields to ApplyMsg. Your solution is complete when it passes all of the Lab 3 tests.

### Hint

1. Think about when a kvserver should snapshot its state and what should be included in the snapshot. Raft must store each snapshot in the persister object using SaveStateAndSnapshot(), along with corresponding Raft state. You can read the latest stored snapshot using ReadSnapshot().
2. Your kvserver must be able to detect duplicated operations in the log across checkpoints, so any state you are using to detect them must be included in the snapshots.
3. Capitalize all fields of structures stored in the snapshot.
4. You are allowed to add methods to your Raft so that kvserver can manage the process of trimming the Raft log and manage kvserver snapshots.
5. Send the entire snapshot in a single InstallSnapshot RPC. Don't implement Figure 13's offset mechanism for splitting up the snapshot.
6. Make sure you pass TestSnapshotRPC before moving on to the other Snapshot tests.
7. A reasonable amount of time to take for the Lab 3 tests is 400 seconds of real time and 700 seconds of CPU time. Further, go test -run TestSnapshotSize should take less than 20 seconds of real time.

### 课堂笔记

#### 日志快照

这也是一种优化手段，使用快照可以快速恢复Raft服务器，要不然总不能从日志的一开始进行恢复吧，那么如果日志有非常多，恢复的速度就会很慢了。

快照背后的思想是，要求应用程序将其状态的拷贝作为一种特殊的Log条目存储下来。我们之前几乎都忽略了应用程序，但是事实是，假设我们基于Raft构建一个key-value数据库，Log将会包含一系列的Put/Get或者Read/Write请求。假设一条Log包含了一个Put请求，客户端想要将X设置成1，另一条Log想要将X设置成2，下一条将Y设置成7。

Log日志如下：
- x = 1
- x = 2
- y = 7

对于Raft上面的KV数据库来说，内容如下：
- x = 2
- y = 7

因为Log是所有命令的集合，因此对于x这个key，可能有N多次修改，因此，就会有N条Log，但是上层的KV数据库里面只有一个数据而已。

所以，当Log非常大以后，Raft会将Log当前的状态做一个快照，有了快照以后，就可以删除这些无用的Log了。我们还需要为快照标注Log的槽位号.

这样的话，服务器崩溃恢复以后，就不再是依赖Log了，而是会`先加载快照`，然后再恢复快照之后的Log。

提问：快照的创建是否依赖应用程序？

> 肯定依赖。快照生成函数是应用程序的一部分，如果是一个key-value数据库，那么快照生成就是这个数据库的一部分。Raft会通过某种方式调用到应用程序，通知应用程序生成快照，因为只有应用程序自己才知道自己的状态（进而能生成快照）。而通过快照反向生成应用程序状态的函数，同样也是依赖应用程序的。但是这里又有点纠缠不清，因为每个快照又必须与某个Log槽位号对应。[1]

提问：如果RPC消息乱序该怎么处理？

> 是在说Raft论文图13的规则6吗？这里的问题是，你们会在Lab3遇到这个问题，因为RPC系统不是完全的可靠和有序，RPC可以乱序的到达，甚至不到达。你或许发了一个RPC，但是收不到回复，并认为这个消息丢失了，但是消息实际上送达了，实际上是回复丢失了。所有这些都可能发生，包括发生在InstallSnapshot RPC中。Leader几乎肯定会并发发出大量RPC，其中包含了AppendEntries和InstallSnapshot，因此，Follower有可能受到一条很久以前的InstallSnapshot消息。因此，Follower必须要小心应对InstallSnapshot消息。我认为，你想知道的是，如果Follower收到了一条InstallSnapshot消息，但是这条消息看起来完全是冗余的，这条InstallSnapshot消息包含的信息比当前Follower的信息还要老，这时，Follower该如何做？Raft论文图13的规则6有相应的说明。我认为正常的响应是，Follower可以忽略明显旧的快照。其实我（Robert教授）看不懂那条规则6。

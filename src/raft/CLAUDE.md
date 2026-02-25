# CLAUDE.md

下面是Raft论文中的描述，请严格遵守以下描述。记住，是MUST，不是Should。

## State

Persistent state on all servers:
(Updated on stable storage before responding to RPCs)
currentTerm latest term server has seen (initialized to 0
on first boot, increases monotonically)
votedFor candidateId that received vote in current
term (or null if none)
log[] log entries; each entry contains command
for state machine, and term when entry
was received by leader (first index is 1)
Volatile state on all servers:
commitIndex index of highest log entry known to be
committed (initialized to 0, increases
monotonically)
lastApplied index of highest log entry applied to state
machine (initialized to 0, increases
monotonically)
Volatile state on leaders:
(Reinitialized after election)
nextIndex[] for each server, index of the next log entry
to send to that server (initialized to leader
last log index + 1)
matchIndex[] for each server, index of highest log entry
known to be replicated on server
(initialized to 0, increases monotonically) 

## AppendEntries RPC

Invoked by leader to replicate log entries (§5.3); also used as
heartbeat (§5.2).
Arguments:
term leader’s term
leaderId so follower can redirect clients
prevLogIndex index of log entry immediately preceding
new ones
prevLogTerm term of prevLogIndex entry
entries[] log entries to store (empty for heartbeat;
may send more than one for efficiency)
leaderCommit leader’s commitIndex
Results:
term currentTerm, for leader to update itself
success true if follower contained entry matching
prevLogIndex and prevLogTerm
Receiver implementation:
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn’t contain an entry at prevLogIndex
whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index
but different terms), delete the existing entry and all that
follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex =
min(leaderCommit, index of last new entry)

## RequestVote RPC

Invoked by candidates to gather votes (§5.2).
Arguments:
term candidate’s term
candidateId candidate requesting vote
lastLogIndex index of candidate’s last log entry (§5.4)
lastLogTerm term of candidate’s last log entry (§5.4)
Results:
term currentTerm, for candidate to update itself
voteGranted true means candidate received vote
Receiver implementation:
1. Reply false if term < currentTerm (§5.1)
2. If votedFor is null or candidateId, and candidate’s log is at
least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

## Rules For Servers

### All Servers
• If commitIndex > lastApplied: increment lastApplied, apply
log[lastApplied] to state machine (§5.3)
• If RPC request or response contains term T > currentTerm:
set currentTerm = T, convert to follower (§5.1)

### Followers (§5.2):
• Respond to RPCs from candidates and leaders
• If election timeout elapses without receiving AppendEntries
RPC from current leader or granting vote to candidate:
convert to candidate

### Candidates (§5.2):
• On conversion to candidate, start election:
• Increment currentTerm
• Vote for self
• Reset election timer
• Send RequestVote RPCs to all other servers
• If votes received from majority of servers: become leader
• If AppendEntries RPC received from new leader: convert to
follower
• If election timeout elapses: start new election

### Leaders:
• Upon election: send initial empty AppendEntries RPCs
(heartbeat) to each server; repeat during idle periods to
prevent election timeouts (§5.2)
• If command received from client: append entry to local log,
respond after entry applied to state machine (§5.3)
• If last log index ≥ nextIndex for a follower: send
AppendEntries RPC with log entries starting at nextIndex
• If successful: update nextIndex and matchIndex for
follower (§5.3)
• If AppendEntries fails because of log inconsistency:
decrement nextIndex and retry (§5.3)
• If there exists an N such that N > commitIndex, a majority
of matchIndex[i] ≥ N, and log[N].term == currentTerm:
set commitIndex = N (§5.3, §5.4).


## SafeTy

The previous sections described how Raft elects leaders and replicates log entries. However, the mechanisms
described so far are not quite sufficient to ensure that each
state machine executes exactly the same commands in the
same order. For example, a follower might be unavailable
while the leader commits several log entries, then it could
be elected leader and overwrite these entries with new
ones; as a result, different state machines might execute
different command sequences.
This section completes the Raft algorithm by adding a
restriction on which servers may be elected leader. The
restriction ensures that the leader for any given term contains all of the entries committed in previous terms (the
Leader Completeness Property from Figure 3). Given the
election restriction, we then make the rules for commitment more precise. Finally, we present a proof sketch for
the Leader Completeness Property and show how it leads
to correct behavior of the replicated state machine.

### Election restriction

In any leader-based consensus algorithm, the leader
must eventually store all of the committed log entries. In
some consensus algorithms, such as Viewstamped Replication [22], a leader can be elected even if it doesn’t
initially contain all of the committed entries. These algorithms contain additional mechanisms to identify the
missing entries and transmit them to the new leader, either during the election process or shortly afterwards. Unfortunately, this results in considerable additional mechanism and complexity. Raft uses a simpler approach where
it guarantees that all the committed entries from previous terms are present on each new leader from the moment of
its election, without the need to transfer those entries to
the leader. This means that log entries only flow in one direction, from leaders to followers, and leaders never overwrite existing entries in their logs.

Raft uses the voting process to prevent a candidate from
winning an election unless its log contains all committed
entries. A candidate must contact a majority of the cluster
in order to be elected, which means that every committed
entry must be present in at least one of those servers. If the
candidate’s log is at least as up-to-date as any other log
in that majority (where “up-to-date” is defined precisely
below), then it will hold all the committed entries. The
RequestVote RPC implements this restriction: the RPC
includes information about the candidate’s log, and the
voter denies its vote if its own log is more up-to-date than
that of the candidate.
Raft determines which of two logs is more up-to-date
by comparing the index and term of the last entries in the
logs. If the logs have last entries with different terms, then
the log with the later term is more up-to-date. If the logs
end with the same term, then whichever log is longer is
more up-to-date.

### Figure 8

A time sequence showing why a leader cannot determine commitment using log entries from older terms. In
(a) S1 is leader and partially replicates the log entry at index
2. In (b) S1 crashes; S5 is elected leader for term 3 with votes
from S3, S4, and itself, and accepts a different entry at log
index 2. In (c) S5 crashes; S1 restarts, is elected leader, and
continues replication. At this point, the log entry from term 2
has been replicated on a majority of the servers, but it is not
committed. If S1 crashes as in (d), S5 could be elected leader
(with votes from S2, S3, and S4) and overwrite the entry with
its own entry from term 3. However, if S1 replicates an entry from its current term on a majority of the servers before
crashing, as in (e), then this entry is committed (S5 cannot
win an election). At this point all preceding entries in the log
are committed as well.

## Livelocks

When your system livelocks, every node in your system is doing something, but collectively your nodes are in such a state that no progress is being made. This can happen fairly easily in Raft, especially if you do not follow Figure 2 religiously. One livelock scenario comes up especially often; no leader is being elected, or once a leader is elected, some other node starts an election, forcing the recently elected leader to abdicate immediately.

There are many reasons why this scenario may come up, but there is a handful of mistakes that we have seen numerous students make:

Make sure you reset your election timer exactly when Figure 2 says you should. Specifically, you should only restart your election timer if a) you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer); b) you are starting an election; or c) you grant a vote to another peer.

This last case is especially important in unreliable networks where it is likely that followers have different logs; in those situations, you will often end up with only a small number of servers that a majority of servers are willing to vote for. If you reset the election timer whenever someone asks you to vote for them, this makes it equally likely for a server with an outdated log to step forward as for a server with a longer log.

In fact, because there are so few servers with sufficiently up-to-date logs, those servers are quite unlikely to be able to hold an election in sufficient peace to be elected. If you follow the rule from Figure 2, the servers with the more up-to-date logs won’t be interrupted by outdated servers’ elections, and so are more likely to complete the election and become the leader.

Follow Figure 2’s directions as to when you should start an election. In particular, note that if you are a candidate (i.e., you are currently running an election), but the election timer fires, you should start another election. This is important to avoid the system stalling due to delayed or dropped RPCs.
Ensure that you follow the second rule in “Rules for Servers” before handling an incoming RPC. The second rule states:

If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

For example, if you have already voted in the current term, and an incoming RequestVote RPC has a higher term that you, you should first step down and adopt their term (thereby resetting votedFor), and then handle the RPC, which will result in you granting the vote!

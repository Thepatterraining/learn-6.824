# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a MIT 6.824 Distributed Systems course implementation containing multiple labs:

- **Lab 1 (MapReduce)**: Distributed MapReduce implementation with master/worker architecture
- **Lab 2 (Raft)**: Raft consensus algorithm implementation
- **Lab 3 (KVRaft)**: Key-value service built on Raft
- **Lab 4a (ShardMaster)**: Sharding configuration service
- **Lab 4b (ShardKV)**: Sharded key-value store

## Architecture

### MapReduce (Lab 1)
- `src/mr/master.go`: Master node coordinating map/reduce tasks
- `src/mr/worker.go`: Worker nodes executing map/reduce functions
- `src/mr/rpc.go`: RPC definitions for master-worker communication
- `src/main/mrmaster.go`: Master process entry point
- `src/main/mrworker.go`: Worker process entry point
- `src/mrapps/`: MapReduce applications (wc.go, indexer.go, etc.) built as plugins

### Core Components
- `src/raft/`: Raft consensus implementation
- `src/labrpc/`: Lab-specific RPC framework
- `src/labgob/`: Lab-specific encoding utilities
- `src/kvraft/`: Key-value service using Raft
- `src/shardmaster/`: Shard configuration management
- `src/shardkv/`: Sharded key-value implementation

## Build Commands

**Note**: Go may not be properly configured in this environment. Commands assume Go is available.

### MapReduce (Lab 1)
```bash
# Build MapReduce applications as plugins
cd src/mrapps
go build -buildmode=plugin wc.go
go build -buildmode=plugin indexer.go

# Build main executables
cd src/main
go build mrmaster.go
go build mrworker.go
go build mrsequential.go
```

### Other Labs
```bash
# Build and test Raft
cd src/raft
go test -c

# Build and test KVRaft
cd src/kvraft
go test -c

# Build and test ShardMaster
cd src/shardmaster
go test -c

# Build and test ShardKV
cd src/shardkv
go test -c
```

## Testing

### MapReduce Testing
```bash
cd src/main
./test-mr.sh
```

The test script:
- Builds all MapReduce plugins and executables
- Runs sequential version for correctness comparison
- Starts master with multiple workers
- Tests various scenarios including crash recovery

### Lab Testing
Each lab has its own test suite:
```bash
cd src/raft && go test
cd src/kvraft && go test
cd src/shardmaster && go test
cd src/shardkv && go test
```

## Submission

Use the Makefile for lab submissions:
```bash
make lab1    # Submit Lab 1 (MapReduce)
make lab2a   # Submit Lab 2A (Raft leader election)
make lab2b   # Submit Lab 2B (Raft log replication)
# etc.
```

The `.check-build` script validates submissions by building with reference test files.

## Development Notes

- Code contains Chinese comments - this is normal for this student's implementation
- The codebase uses Go modules (`go.mod` specifies Go 1.25.1)
- MapReduce apps are built as plugins and loaded dynamically
- RPC communication uses the custom `labrpc` package, not standard Go RPC
- Temporary files are created in `mr-tmp/` directories during testing

## rule

- must don't edit my code,only show your code
- 不要修改代码
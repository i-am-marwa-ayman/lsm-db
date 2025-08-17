#  LSM-DB

A lightweight, educational implementation of an LSM-tree based key-value database. Built from scratch in Go to understand the fundamental concepts behind modern database storage engines.


## Features

- **LSM-Tree Architecture**: Multi-level storage with automatic compaction
- **In-Memory AVL Tree**: Self-balancing binary search tree for active data
- **Block-based SSTable Format**: Efficient disk storage
- **Automatic Compaction**: Background merging of SSTables across levels
- **Tombstone Deletion**: Proper handling of deleted keys
- **Iterator Pattern**: Efficient sequential access to SSTable data
- **Binary Search Optimization**: Fast key lookups within blocks
- **CRUD Operations**: Complete Create, Read, Update, Delete functionality

## Architecture

```
Engine → MemTable (AVL Tree) → SSTable Manager
                                     ↓
                            Level 0: [SSTable] [SSTable]
                            Level 1: [SSTable]
                            Level 2: [SSTable]

```
### SSTable File Structure

```
┌─────────────────┬─────────────────┬─────────────────┐
│   data block    │   data block    │   data block    │
├─────────────────┼─────────────────┼─────────────────┤
│   data block    │   data block    │   data block    │
├─────────────────┼─────────────────┼─────────────────┤
│  index block    │  index block    │  index block    │
├─────────────────┼─────────────────┼─────────────────┤
│  index block    │  index block    │  index block    │
├─────────────────┴─────────────────┴─────────────────┤
│                    footer                           │
└─────────────────────────────────────────────────────┘
```

#### Data Block Structure (fixed size 4K)
```
┌────────┬────────┬────────┬────────┬────────┐
│ entry1 │ entry2 │ entry3 │ entry4 │ entryN │
└────────┴────────┴────────┴────────┴────────┘
```

#### Entry Structure (variable size)
```
┌─────────┬───────┬─────────┬───────┬───────────┬─────────┐
│ key len │  key  │ val len │  val  │ timestamp │ deleted │
└─────────┴───────┴─────────┴───────┴───────────┴─────────┘
```

#### Index Block Structure (variable size)
```
┌─────────────────────────────────────────────────────────────┐
│  block size, entry count                                    │
│  offset1, key1, offset2, key2, offset3, key3, offsetN, keyN │
└─────────────────────────────────────────────────────────────┘
```

#### Footer Structure (variable size)
```
┌─────────────────────────────────────────────────────────────┐
│  index1 offset, index2 offset, index3 offset,               │
│  indexN offset, blockCount                                  │
└─────────────────────────────────────────────────────────────┘
```


### Data Flow
1. **Write Path**: Data → MemTable → SSTable (Level 0) → Compaction
2. **Read Path**: MemTable → Level 0 → Level 1 → Level 2...
3. **Compaction**: 2 SSTables per level trigger merge to next level
4. **Search**: Two level binary search on data index, then direct offset lookup


## Build & Run

```bash
git clone https://github.com/i-am-marwa-ayman/lsm-db
cd lsm-db
go run main.go
```

## Future Work
- [ ] **Persistence & Recovery**: Rebuild state on startup
- [ ] **Write-Ahead Logging**: Crash recovery for uncommitted data  
- [ ] **Bloom Filters**: Fast negative lookups
- [x] **Block-based Storage**: Better I/O efficiency
- [ ] **Range Queries**: Iterator support for key ranges
- [ ] **Concurrency**: Thread-safe operations


## Feedback

This is an educational project built to understand LSM-tree internals. If you find any issues, have suggestions for improvements, or want to discuss the implementation:
- Open an issue on GitHub
- Reach out via email or my [x](https://x.com/_Marwa_Ayman_) account

---

*Educational project to understand LSM-tree databases like LevelDB, RocksDB, and Cassandra.*


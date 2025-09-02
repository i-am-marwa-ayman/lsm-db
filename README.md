#  LSM-DB

A lightweight, educational implementation of an LSM-tree based key-value database. Built from scratch in Go to understand the fundamental concepts behind modern database storage engines.


## Features

- **LSM-Tree Architecture** with multi-level storage and automatic compaction
- **AVL Tree MemTable** for balanced in-memory operations
- **Block-based SSTables** with 4KB fixed-size data blocks
- **Sparse Indexing** for memory-efficient lookups
- **Automatic Compaction** across storage levels
- **Tombstone Deletions** with proper cleanup
- **Binary Search** optimization throughout
- **State Persistence** with manifest-based recovery

## Architecture

```
Engine → MemTable (AVL Tree) → SSTable Manager
                                     ↓
                            Level 0: [SSTable] [SSTable]
                            Level 1: [SSTable]
                            Level 2: [SSTable]
                                     ↓
                              Manifest File
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

#### Data Block Structure (fixed size 4KB)
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

#### Sparse Index Block Structure (variable size)
```
┌─────────────────────────────────────────────────────────────┐
│  block size, entry count, min key, max key                  │
│  [sparse entries at configurable intervals]                 │
│  offset1, key1, offset2, key2, ..., offsetN, keyN           │
└─────────────────────────────────────────────────────────────┘
```

#### Footer Structure (variable size)
```
┌─────────────────────────────────────────────────────────────┐
│ indexBlock1 offset, indexBlock2 offset, indexBlock3 offset, │
│ indexN offset, block count                                  │
└─────────────────────────────────────────────────────────────┘
```

### Sparse Indexing System
- **Configurable Intervals**: Index entries created every N records (default: 10)
- **Memory Efficiency**: Reduces index size while maintaining fast lookups
- **Two-Level Search**: Block-level binary search followed by sparse index navigation
- **Range Optimization**: Min/max keys in each block enable quick range elimination

### Persistence & Recovery
- **Manifest File**: Tracks all SSTable files and their organization across levels
- **Automatic Recovery**: Database state restored from disk on startup
- **Footer-based Restoration**: Index blocks rebuilt from SSTable footers


### Data Flow
1. **Write Path**: Data → MemTable → SSTable (Level 0) → Compaction
2. **Read Path**: MemTable → Level 0 → Level 1 → Level 2...
3. **Compaction**: 2 SSTables per level trigger merge to next level
4. **Search**: Block binary search → Sparse index navigation → Linear search within interval range
5. **Recovery**: Manifest parsing → SSTable restoration → Index rebuilding


## Build & Run

```bash
git clone https://github.com/i-am-marwa-ayman/lsm-db
cd lsm-db
go run main.go
```

## Configuration

The database can be configured via the `shared.Config` struct:

```go
type Config struct {
    MAX_IN_DISK_PAGE_SIZE int32  // Block size (default: 4KB)
    MAX_IN_MEMORY_SIZE    int32  // MemTable size limit (default: 16KB)
    SPARSE_INDEX_INTERVAL int32  // Indexing interval (default: 10)
    DATA_PATH             string // Storage directory
}
```

## File Structure

After running, the following files are created in the data directory:

```
data/
├── manifest           # Database structure metadata
├── 0.0.data          # Level 0 SSTable files
├── 0.1.data
├── 1.0.data          # Level 1 SSTable files
└── ...
```

## Future Work
- [ ] **Write-Ahead Logging**: Crash recovery for uncommitted data  
- [ ] **Bloom Filters**: Fast negative lookups
- [x] **Block-based Storage**: Better I/O efficiency ✓
- [x] **Sparse Indexing**: Memory-efficient index structures ✓
- [x] **Persistence & Recovery**: Rebuild state on startup ✓
- [ ] **Range Queries**: Iterator support for key ranges
- [ ] **Concurrency**: Thread-safe operations with finer-grained locking
- [ ] **Client-Server Architecture**: Database server with CLI client interface

## Feedback

This is an educational project built to understand LSM-tree internals. If you find any issues, have suggestions for improvements, or want to discuss the implementation:
- Open an issue on GitHub
- Reach out via email or my X account

---

*Educational project to understand LSM-tree databases like LevelDB, RocksDB, and Cassandra.*
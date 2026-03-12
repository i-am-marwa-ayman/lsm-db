# LSM-DB

A lightweight, educational implementation of an LSM-tree based key-value database. Built from scratch in Go to understand the fundamental concepts behind modern database storage engines.


## Features

- **LSM-Tree Architecture** — multi-level storage with automatic compaction
- **AVL Tree MemTable** — balanced in-memory operations
- **Write-Ahead Log (WAL)** — crash recovery for uncommitted writes
- **Block-based SSTables** — fixed 4KB data blocks for efficient I/O
- **Sparse Index** — memory-efficient indexing at configurable intervals
- **Tombstone Deletions** — logical deletes with compaction-time cleanup
- **Binary Search** — fast block and index-level lookups
- **Manifest-based Recovery** — full state reconstruction on startup


## Architecture

```
Write Path:
  Set(key, val) / Delete(key)
      │
      ├─► WAL (append-only log for crash safety)
      │
      └─► MemTable (AVL Tree, in-memory)
              │
              │ [full → flush]
              ▼
          SSTable Level 0
              │
              │ [2 SSTables → compact]
              ▼
          SSTable Level 1
              │
              │ [2 SSTables → compact]
              ▼
          SSTable Level 2  ← tombstones purged here (if the last level)

Read Path:
  Get(key)
      │
      ├─► MemTable  (most recent)
      ├─► Level 0   (newest SSTables first)
      ├─► Level 1
      └─► Level 2   (oldest data)
```


## SSTable File Format

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

### Entry Structure (variable size)
```
┌─────────┬───────┬─────────┬───────┬───────────┬─────────┐
│ key len │  key  │ val len │  val  │ timestamp │ deleted │
└─────────┴───────┴─────────┴───────┴───────────┴─────────┘
```

### Index Block Layout

Each index block corresponds to one data block and records:
- `blockEntriesCount` — number of entries in the data block
- `blockSize` — actual byte size of the data (excluding padding)
- `minKey` / `maxKey` — key range for fast block elimination
- Sparse index entries at every `SPARSE_INDEX_INTERVAL` records:
  `[ offset | key_len | key ]`

### Footer Layout

```
[ index_block_0_offset | index_block_1_offset | ... | block_count ]
```


## Lookup Algorithm

1. **Block search** — binary search across index blocks using `minKey`/`maxKey` to find the candidate data block. O(log B) where B = number of blocks.
2. **Sparse index navigation** — within the chosen index block, binary search the sparse entries to narrow the byte range to search. O(log(N / interval)).
3. **Linear scan** — sequential scan within the identified byte window (at most `SPARSE_INDEX_INTERVAL` entries). O(interval).


## Compaction

- When any level accumulates **2 SSTables**, they are merged into the next level.
- Merging is a sort-merge join on sorted key order. For duplicate keys, the entry from the newer SSTable wins.
- Tombstones (deleted keys) are preserved through intermediate levels and **purged only at the last level**, ensuring deleted keys are not resurrected from older SSTables.
- If compaction produces an empty SSTable (all entries were tombstones), the file is discarded.


## Write-Ahead Log (WAL)

When `ENABLE_WAL` is true, every write (`Set` / `Delete`) is appended to `wal.log` before being applied to the MemTable. On startup, the WAL is replayed into a fresh MemTable to recover any writes that were not yet flushed to disk. The WAL is truncated after each successful flush.


## Persistence & Recovery

The `manifest` file records the number of levels and the number of SSTables per level. On startup:

1. The manifest is read to determine the SSTable layout.
2. Each SSTable's footer is parsed to locate its index blocks.
3. Index blocks are reconstructed in memory, ready for lookups.

No full file scan is needed at startup.


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
    ENABLE_WAL            bool   // Enable write-ahead log (default: true)
}
```


## File Structure

```
data/
├── wal.log           # Write-ahead log for crash recovery
├── manifest          # Database structure metadata
├── 0.0.data          # Level 0 SSTable files
├── 0.1.data
├── 1.0.data          # Level 1 SSTable files
└── ...
```

SSTable filenames follow the pattern `{level}.{index}.data`.


## Future Work

- [x] **Write-Ahead Logging** — crash recovery for uncommitted data
- [x] **Block-based Storage** — better I/O efficiency
- [x] **Sparse Indexing** — memory-efficient index structures
- [x] **Persistence & Recovery** — rebuild state on startup
- [ ] **Bloom Filters** — fast negative lookups
- [ ] **Range Queries** — iterator support for key ranges
- [ ] **Concurrency** — thread-safe operations with finer-grained locking
- [ ] **Client-Server Architecture** — database server with CLI client interface


## Feedback

This is an educational project built to understand LSM-tree internals. If you find any issues, have suggestions for improvements, or want to discuss the implementation:
- Open an issue on GitHub
- Reach out via email or my X account

---

*Educational project to understand LSM-tree databases like LevelDB, RocksDB, and Cassandra.*
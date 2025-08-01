#  LSM-DB

A lightweight, educational implementation of an LSM-tree based key-value database, inspired by Google's LevelDB. Built from scratch in Go to understand the fundamental concepts behind modern database storage engines.


## Features

- **LSM-Tree Architecture**: Multi-level storage with automatic compaction
- **In-Memory AVL Tree**: Self-balancing binary search tree for active data
- **Binary SSTable Format**: Efficient disk storage with sequential writes
- **Automatic Compaction**: Background merging of SSTables across levels
- **Tombstone Deletion**: Proper handling of deleted keys
- **Reader/Writer Pattern**: Clean separation of file operations
- **CRUD Operations**: Complete Create, Read, Update, Delete functionality

## Architecture

```
Engine → MemTable (AVL Tree) → SSTable Manager
                                     ↓
                            Level 0: [SSTable] [SSTable]
                            Level 1: [SSTable]
                            Level 2: [SSTable]
```

### SSTable Format

```
┌────────────────────────────────────────────────────────┐
│ Header: Entry Count (8 bytes)                          │
├────────────────────────────────────────────────────────┤
│ Entry 1: [Size][Key_Len][Key][Val_Len][Val][Time][Del] │
├────────────────────────────────────────────────────────┤
│ Entry 2: [Size][Key_Len][Key][Val_Len][Val][Time][Del] │
├────────────────────────────────────────────────────────┤
│ ...                                                    │
└────────────────────────────────────────────────────────┘
```

### Data Flow
1. **Write Path**: Data → MemTable → SSTable (Level 0) → Compaction
2. **Read Path**: MemTable → Level 0 → Level 1 → Level 2...
3. **Compaction**: 2 SSTables per level trigger merge to next level

## Usage

```go
db := engine.NewEngine()

// Basic operations
db.Set("name", "marwa")
db.Set("language", "go")
value, _ := db.Get("name")  // Returns "marwa"
db.Delete("language")

// Automatic compaction happens when memtable fills up
```
## Project Structure

```
lsm-db/
├── engine/
│   └── engine.go          # Main database interface
├── memtable/
│   ├── memtable.go        # MemTable management
│   ├── avl.go             # AVL tree implementation
│   └── entry.go           # Data entry structure
├── sstable/
│   ├── sstable.go         # SSTable operations
│   ├── reader.go          # File reading operations
│   ├── writer.go          # File writing operations
│   ├── compactor.go       # Compaction logic
│   └── ssManager.go       # Multi-level management
└── main.go                # Example usage
```

## Future Work
- [ ] **Persistence & Recovery**: Rebuild state on startup
- [ ] **Write-Ahead Logging**: Crash recovery for uncommitted data  
- [ ] **Bloom Filters**: Fast negative lookups
- [ ] **Block-based Storage**: Better I/O efficiency
- [ ] **Range Queries**: Iterator support for key ranges
- [ ] **Concurrency**: Thread-safe operations

## Build & Run
**Warning** make sure to change the sstable dir in ssmanager.go
```bash
git clone https://github.com/i-am-marwa-ayman/lsm-db
cd lsm-db
go run main.go
```

---

*Educational project to understand LSM-tree databases like LevelDB, RocksDB, and Cassandra.*
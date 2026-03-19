package memtable

import (
	"fmt"
	"sync"

	"github.com/i-am-marwa-ayman/lsm-db/shared"
)

// why did i separate avl from memtable?
// to make it easier if i change the avl to other ds (skip list)

type MemTable struct {
	root    *avl
	size    int32
	maxSize int32
	cfg     *shared.Config
	lock    *sync.RWMutex
}

func NewMemtable(cfg *shared.Config) *MemTable {
	return &MemTable{
		root:    nil,
		size:    0,
		maxSize: cfg.MAX_IN_MEMORY_SIZE,
		cfg:     cfg,
		lock:    &sync.RWMutex{},
	}
}
func (mt *MemTable) Size() int32 {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	return mt.size
}
func (mt *MemTable) IsFull() bool {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	return mt.maxSize <= mt.size
}
func (mt *MemTable) IsEmpty() bool {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	return mt.size == 0
}
func (mt *MemTable) SetAll(entries []*shared.Entry) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	for _, entry := range entries {
		err := mt.addEntry(entry)
		if err != nil {
			return err
		}
	}
	return nil
}
func (mt *MemTable) Get(key []byte) *shared.Entry {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	entry := mt.root.LookUp(key)
	return entry
}
func (mt *MemTable) Set(key []byte, val []byte) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	newEntry := shared.NewEntry(key, val)
	return mt.addEntry(newEntry)
}
func (mt *MemTable) Delete(key []byte) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	newEntry := shared.DeletedEntry(key)
	return mt.addEntry(newEntry)
}
func (mt *MemTable) GetAll() []*shared.Entry {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	return mt.root.GetAll()
}

func (mt *MemTable) addEntry(entry *shared.Entry) error {
	if entry.Size() > int(mt.cfg.MAX_IN_DISK_PAGE_SIZE) {
		return fmt.Errorf("entry size exceeds max page size")
	}
	newAdd := 0
	mt.root, newAdd = mt.root.Insert(entry)
	mt.size += int32(newAdd) // will add entry size if we update non-existing val in memtable (we will add the diff if existing)
	return nil
}

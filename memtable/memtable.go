package memtable

import (
	"fmt"

	"github.com/i-am-marwa-ayman/lsm-db/shared"
)

type MemTable struct {
	root    *AVL
	size    int32
	maxSize int32
	cfg     *shared.Config
}

func NewMemtable(cfg *shared.Config) *MemTable {
	return &MemTable{
		root:    nil,
		size:    0,
		maxSize: cfg.MAX_IN_MEMORY_SIZE,
		cfg:     cfg,
	}
}
func (mt *MemTable) Size() int32 {
	return mt.size
}
func (mt *MemTable) IsFull() bool {
	return mt.maxSize <= mt.size
}
func (mt *MemTable) Get(key []byte) *Entry {
	entry := mt.root.LookUp(key)
	return entry
}
func (mt *MemTable) Set(key []byte, val []byte) error {
	nEntry := NewEntry(key, val)
	if nEntry.size() > int(mt.cfg.MAX_IN_DISK_PAGE_SIZE) {
		return fmt.Errorf("entry size exceed limit size")
	}
	newAdd := 0
	mt.root, newAdd = mt.root.Insert(key, nEntry)
	mt.size += int32(newAdd) // will add entry size if we update non-existing val in memtable
	return nil
}
func (mt *MemTable) Delete(key []byte) error {
	nEntry := DeletedEntry(key)
	if nEntry.size() > int(mt.cfg.MAX_IN_DISK_PAGE_SIZE) {
		return fmt.Errorf("entry size exceed limit size")
	}
	newAdd := 0
	mt.root, newAdd = mt.root.Insert(key, nEntry)
	mt.size += int32(newAdd)
	return nil
}
func (mt *MemTable) GetAll() []*Entry {
	return mt.root.GetAll()
}

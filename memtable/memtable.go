package memtable

import (
	"time"
)

type Entry struct {
	Key       string
	Value     string
	Tombstone bool
	Timestamp int
}

func NewEntry(key string, val string) *Entry {
	return &Entry{
		Key:       key,
		Value:     val,
		Tombstone: false,
		Timestamp: int(time.Now().Unix()),
	}
}
func DeletedEntry(key string) *Entry {
	return &Entry{
		Key:       key,
		Value:     "",
		Tombstone: true,
		Timestamp: int(time.Now().Unix()),
	}
}

type MemTable struct {
	root    *AVL
	size    int
	maxSize int
}

func NewMemtable() *MemTable {
	return &MemTable{
		root:    nil,
		size:    0,
		maxSize: 1000,
	}
}
func (mt *MemTable) Get(key string) *Entry {
	nEntry := mt.root.LookUp(key)
	return nEntry
}
func (mt *MemTable) Set(key string, val string) {
	nEntry := NewEntry(key, val)
	mt.root = mt.root.Insert(key, nEntry)
}
func (mt *MemTable) Delete(key string) {
	nEntry := DeletedEntry(key)
	mt.root = mt.root.Insert(key, nEntry)
}

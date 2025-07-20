package lsm

import (
	"time"
)

type MemTable struct {
	avl     *AVL
	size    int
	maxSize int
}
type Entry struct {
	Key       string
	Value     string
	Tombstone bool
	Timestamp int
}

func NewMemtable() *MemTable {
	return &MemTable{
		avl:     nil,
		size:    0,
		maxSize: 1000,
	}
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

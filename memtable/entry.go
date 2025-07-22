package memtable

import (
	"time"
)

type Entry struct {
	Key       string
	Value     string
	Tombstone bool
	Timestamp int64
}

func NewEntry(key string, val string) *Entry {
	return &Entry{
		Key:       key,
		Value:     val,
		Tombstone: false,
		Timestamp: int64(time.Now().Unix()),
	}
}
func DeletedEntry(key string) *Entry {
	return &Entry{
		Key:       key,
		Value:     "",
		Tombstone: true,
		Timestamp: int64(time.Now().Unix()),
	}
}

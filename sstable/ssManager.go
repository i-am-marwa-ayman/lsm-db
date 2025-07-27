package sstable

import (
	"fmt"
	"mini-levelDB/memtable"
)

type SsManager struct {
	sstables []*sstable
}

func NewSsManager() *SsManager {
	return &SsManager{
		sstables: make([]*sstable, 0),
	}
}
func (sm *SsManager) AddSstable(allEntries []*memtable.Entry) error {
	st := newSstable(fmt.Sprintf("%d.data", len(sm.sstables)))
	err := st.writeData(allEntries)
	sm.sstables = append(sm.sstables, st)
	return err
}
func (sm *SsManager) Get(key string) (string, error) {
	for i := len(sm.sstables) - 1; i >= 0; i-- {
		sstable := sm.sstables[i]
		if entry, err := sstable.get(key); err == nil {
			if entry.Tombstone {
				return "", fmt.Errorf("no such a key")
			} else {
				return entry.Value, nil
			}
		}
	}
	return "", fmt.Errorf("no such a key")
}

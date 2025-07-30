package sstable

import (
	"fmt"
	"log"
	"mini-levelDB/memtable"
)

type SsManager struct {
	sstables [][]*sstable
}

func NewSsManager() *SsManager {
	sst := make([][]*sstable, 3)
	for i := range sst {
		sst[i] = make([]*sstable, 0)
	}
	return &SsManager{
		sstables: sst,
	}
}
func (sm *SsManager) AddSstable(allEntries []*memtable.Entry) error {
	st := newSstable(fmt.Sprintf("../../data/0.%d.data", len(sm.sstables[0])))
	err := st.writeData(allEntries)
	if err != nil {
		return err
	}
	sm.sstables[0] = append(sm.sstables[0], st)
	err = sm.fixLevels()
	fmt.Println("sstable layout")
	fmt.Println(len(sm.sstables))
	for i, level := range sm.sstables {
		fmt.Printf("in level %d: %d sstabless\n", i, len(level))
	}
	return err
}
func (sm *SsManager) fixLevels() error {
	for i, level := range sm.sstables {
		if len(level) == 2 {
			log.Printf("start compact level %d to level %d...", i, i+1)
			if len(sm.sstables) == i+1 {
				nlevel := make([]*sstable, 0)
				sm.sstables = append(sm.sstables, nlevel)
			}
			nst := newSstable(fmt.Sprintf("../../data/%d.%d.data", i+1, len(sm.sstables[i+1])))
			err := nst.Compact(level[0], level[1])
			if err != nil {
				return err
			}
			sm.sstables[i+1] = append(sm.sstables[i+1], nst)
			sm.sstables[i] = make([]*sstable, 0)
			log.Printf("level %d compacted successfully", i)
		}
	}
	return nil
}
func (sm *SsManager) Get(key string) *memtable.Entry {
	// searching first row
	for _, level := range sm.sstables {
		for i := len(level) - 1; i >= 0; i-- {
			sstable := level[i]
			if entry, err := sstable.get(key); err == nil {
				fmt.Printf("key founded in sstable: %s\n", sstable.fileName)
				return entry
			}
		}
	}
	return nil
}

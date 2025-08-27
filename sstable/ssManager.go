package sstable

import (
	"fmt"
	"log"
	"os"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
)

type SsManager struct {
	sstables [][]*sstable
	dataPath string
}

func creatPath(dataPath string) error {
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	return nil
}
func NewSsManager(dataPath string) (*SsManager, error) {
	err := creatPath(dataPath)
	if err != nil {
		return nil, err
	}
	sst := make([][]*sstable, 3)
	for i := range sst {
		sst[i] = make([]*sstable, 0)
	}
	return &SsManager{
		sstables: sst,
		dataPath: dataPath,
	}, nil
}
func (sm *SsManager) AddSstable(entries []*memtable.Entry) error {
	st := newSstable(fmt.Sprintf("%s/0.%d.data", sm.dataPath, len(sm.sstables[0])))
	err := st.writeSstable(entries)
	if err != nil {
		return err
	}
	// st.readSstable()
	sm.sstables[0] = append(sm.sstables[0], st)
	err = sm.fixLevels()
	sm.listSstables()
	return err
}
func (sm *SsManager) listSstables() {
	fmt.Println("sstable layout")
	fmt.Println(len(sm.sstables))
	for i, level := range sm.sstables {
		fmt.Printf("in level %d: %d sstabless\n", i, len(level))
	}
}

func (sm *SsManager) fixLevels() error {
	for i, level := range sm.sstables {
		if len(level) == 2 {
			log.Printf("start compact level %d to level %d...\n", i, i+1)
			deleteZombie := false
			if len(sm.sstables) == i+1 {
				deleteZombie = true
				nlevel := make([]*sstable, 0)
				sm.sstables = append(sm.sstables, nlevel)
			}
			nst := newSstable(fmt.Sprintf("%s/%d.%d.data", sm.dataPath, i+1, len(sm.sstables[i+1])))
			err := nst.compact(level[0], level[1], deleteZombie)
			if err != nil {
				return err
			}
			// nst.readSstable()
			// if newsstable if empty do not add it (all deleted)
			if len(nst.indexBlocks) > 0 {
				sm.sstables[i+1] = append(sm.sstables[i+1], nst)
				sm.sstables[i] = make([]*sstable, 0)
			} else {
				log.Println("delete sstable")
			}
			sm.sstables[i] = make([]*sstable, 0)
			log.Printf("level %d compacted successfully\n", i)
		}
	}
	return nil
}
func (sm *SsManager) Get(key []byte) *memtable.Entry {
	for l, level := range sm.sstables {
		for i := len(level) - 1; i >= 0; i-- {
			sstable := level[i]
			if entry, err := sstable.searchSstable(key); entry != nil {
				if !entry.Tombstone {
					log.Printf("key founded in sstable: %s\n", sstable.fileName)
				}
				return entry
			} else if err != nil {
				log.Printf("error happend in sstable %d.%d: %s\n", l, i, err)
			}
		}
	}
	return nil
}

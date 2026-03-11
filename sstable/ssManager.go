package sstable

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/i-am-marwa-ayman/lsm-db/shared"
)

type SsManager struct {
	sstables [][]*sstable
	cfg      *shared.Config
}

func creatPath(dataPath string) error {
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	return nil
}

func (sm *SsManager) writeManfiestFile() error {
	file, err := os.OpenFile(sm.cfg.DATA_PATH+"/manifest", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	err = binary.Write(file, binary.LittleEndian, int64(len(sm.sstables)))
	if err != nil {
		return fmt.Errorf("failed to write manifest file: %w", err)
	}
	for _, l := range sm.sstables {
		err = binary.Write(file, binary.LittleEndian, int64(len(l)))
		if err != nil {
			return fmt.Errorf("failed to write manifest file: %w", err)
		}
	}
	return nil
}
func NewSsManager(cfg *shared.Config) (*SsManager, error) {
	sm := &SsManager{cfg: cfg}
	err := creatPath(cfg.DATA_PATH)
	if err != nil {
		return nil, err
	}
	sm.sstables, err = sm.recover()
	if err != nil {
		return nil, err
	}
	sm.listSstables()
	return sm, err
}

func (sm *SsManager) Close() error {
	for _, level := range sm.sstables {
		for _, st := range level {
			st.it.close()
		}
	}
	return sm.writeManfiestFile()
}
func (sm *SsManager) AddSstable(entries []*shared.Entry) error {
	st := sm.newSstable(sm.cfg.DATA_PATH + "/0." + strconv.Itoa(len(sm.sstables[0])) + ".data")
	err := st.writeSstable(entries)
	if err != nil {
		return err
	}
	st.it, err = st.newIterator()
	if err != nil {
		return err
	}
	sm.sstables[0] = append(sm.sstables[0], st)
	err = sm.fixLevels()
	sm.listSstables()
	return err
}
func (sm *SsManager) listSstables() {
	log.Println("[SSManager] ==================== SSTable Layout ====================")
	log.Printf("[SSManager] Total levels: %d\n", len(sm.sstables))
	for i, level := range sm.sstables {
		log.Printf("[SSManager]   Level %d: %d SSTables\n", i, len(level))
		for j := range len(level) {
			log.Printf("[SSManager]     └─ SSTable %d: %d blocks\n", j, len(sm.sstables[i][j].indexBlocks))
		}
	}
	log.Println("[SSManager] =========================================================")
}

func (sm *SsManager) fixLevels() error {
	for i, level := range sm.sstables {
		if len(level) == 2 {
			log.Printf("[SSManager] Starting compaction: level %d -> level %d\n", i, i+1)
			deleteZombie := false
			if len(sm.sstables) == i+1 {
				deleteZombie = true
				nlevel := make([]*sstable, 0)
				sm.sstables = append(sm.sstables, nlevel)
			}
			nst := sm.newSstable(sm.cfg.DATA_PATH + "/" + strconv.Itoa(i+1) + "." + strconv.Itoa(len(sm.sstables[i+1])) + ".data")
			err := nst.compact(level[0], level[1], deleteZombie)
			if err != nil {
				return err
			}
			// if newsstable if empty do not add it (all deleted)
			if nst.size > 0 {
				sm.sstables[i+1] = append(sm.sstables[i+1], nst)
				sm.sstables[i] = make([]*sstable, 0)
				nst.it, err = nst.newIterator()
				if err != nil {
					return err
				}
			} else {
				log.Println("[SSManager] Compacted SSTable is empty, skipping write")
			}
			sm.sstables[i] = make([]*sstable, 0)
			log.Printf("[SSManager] Level %d compacted successfully\n", i)
		}
	}
	return nil
}

func (sm *SsManager) Get(key []byte) *shared.Entry {
	for l, level := range sm.sstables {
		for i := len(level) - 1; i >= 0; i-- {
			sstable := level[i]
			if entry, err := sstable.searchSstable(key); entry != nil {
				if !entry.Tombstone {
					log.Printf("[SSManager] Key found in sstable: %s\n", sstable.fileName)
				}
				return entry
			} else if err != nil {
				log.Printf("[SSManager] Error searching sstable %d.%d: %s\n", l, i, err)
			}
		}
	}
	return nil
}

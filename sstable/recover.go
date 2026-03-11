package sstable

import (
	"encoding/binary"
	"errors"
	"log"
	"os"
	"strconv"
)

func (sm *SsManager) defaultSstables() [][]*sstable {
	sst := make([][]*sstable, 3)
	for i := range sst {
		sst[i] = make([]*sstable, 0)
	}
	return sst
}
func (sm *SsManager) recover() ([][]*sstable, error) {
	if _, err := os.Stat(sm.cfg.DATA_PATH + "/manifest"); errors.Is(err, os.ErrNotExist) {
		log.Println("[SSManager] No manifest found, initializing with empty SSTables")
		return sm.defaultSstables(), nil
	}
	log.Println("[SSManager] Starting recovery from manifest...")
	file, err := os.Open(sm.cfg.DATA_PATH + "/manifest")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var levelNum int64
	err = binary.Read(file, binary.LittleEndian, &levelNum)
	if err != nil {
		return nil, err
	}
	log.Printf("[SSManager] Recovering %d levels from manifest\n", levelNum)
	sstables := make([][]*sstable, levelNum)
	for i := range sstables {
		var n int64
		err = binary.Read(file, binary.LittleEndian, &n)
		if err != nil {
			return nil, err
		}
		level := make([]*sstable, n)
		for j := range level {
			nst := sm.newSstable(sm.cfg.DATA_PATH + "/" + strconv.Itoa(i) + "." + strconv.Itoa(j) + ".data")
			err = nst.recover()
			if err != nil {
				return nil, err
			}
			level[j] = nst
		}
		log.Printf("[SSManager] Recovered level %d with %d SSTables\n", i, n)
		sstables[i] = level
	}
	log.Println("[SSManager] Recovery completed successfully")
	if len(sstables) == 0 {
		sstables = sm.defaultSstables()
	}
	return sstables, nil
}

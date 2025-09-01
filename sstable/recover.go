package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
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
		return sm.defaultSstables(), nil
	}
	log.Println("working on recovering...")
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
	sstables := make([][]*sstable, levelNum)
	for i := range sstables {
		var n int64
		err = binary.Read(file, binary.LittleEndian, &n)
		if err != nil {
			return nil, err
		}
		level := make([]*sstable, n)
		for j := range level {
			nst := sm.newSstable(fmt.Sprintf("%s/%d.%d.data", sm.cfg.DATA_PATH, i, j))
			err = nst.recover()
			if err != nil {
				return nil, err
			}
			level[j] = nst
		}
		sstables[i] = level
	}
	log.Println("recovering done")
	if len(sstables) == 0 {
		sstables = sm.defaultSstables()
	}
	return sstables, nil
}

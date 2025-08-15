package sstable

import (
	"fmt"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
)

type sstable struct {
	fileName    string
	indexBlocks []*indexBlock
}

func newSstable(fileName string) *sstable {
	return &sstable{
		fileName:    fileName,
		indexBlocks: make([]*indexBlock, 0),
	}
}
func (st *sstable) readSstable() error {
	for _, b := range st.indexBlocks {
		fmt.Println(len(b.metadataEntries))
	}
	it, err := st.newIterator()
	if err != nil {
		return err
	}
	defer it.close()

	for {
		entry, err := it.next()
		if err != nil {
			return nil
		}
		if entry == nil {
			break
		}
		fmt.Printf("key: %s, val: %s\n", entry.Key, entry.Value)
	}
	return nil
}
func (st *sstable) writeSstable(entries []*memtable.Entry) error {
	w, err := st.newBlockWriter()
	if err != nil {
		return err
	}
	defer w.close()

	for _, ententry := range entries {
		err = w.addEntry(ententry)
		if err != nil {
			return err
		}
	}
	err = w.flushDataBlock() // make sure to flush every entry
	if err != nil {
		return err
	}
	err = w.flushMetadataBlocks()
	if err != nil {
		return err
	}
	st.indexBlocks = w.indexBlocks

	return nil
}
func (st *sstable) getOffset(index int, key string) int16 {
	indexBlock := st.indexBlocks[index]

	low := 0
	high := len(indexBlock.metadataEntries) - 1

	for low <= high {
		mid := (low + high) / 2
		midIndexEntry := indexBlock.metadataEntries[mid]
		if key == midIndexEntry.key {
			return midIndexEntry.offset
		} else if key < midIndexEntry.key {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return -1
}
func (st *sstable) getIndex(key string) int {
	low := 0
	high := len(st.indexBlocks) - 1

	for low <= high {
		mid := (low + high) / 2
		midIndex := st.indexBlocks[mid]
		if midIndex.metadataEntries[0].key <= key && key <= midIndex.metadataEntries[len(midIndex.metadataEntries)-1].key {
			return mid
		} else if key < midIndex.metadataEntries[0].key {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return -1
}

func (st *sstable) searchSstable(key string) (*memtable.Entry, error) {
	r, err := st.newBlockReader()
	if err != nil {
		return nil, err
	}
	defer r.close()
	// TODO: change to binary search
	index := st.getIndex(key)
	if index == -1 {
		return nil, nil
	}
	offset := st.getOffset(index, key)
	if offset == -1 {
		return nil, nil
	}
	return r.readEntryAtOffest(int64(index*MAX_BLOCK_SIZE + int(offset)))
}

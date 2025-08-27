package sstable

import (
	"bytes"
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

	for _, entry := range entries {
		err = w.addEntry(entry)
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

// get data window between two index to search target
func (st *sstable) searchIndex(index int, key []byte) (startOffset int64, size int32) {
	indexBlock := st.indexBlocks[index]
	// block entries than SPARSE_INDEX_INTERVAL
	if len(indexBlock.metadataEntries) == 0 {
		return int64(index * MAX_BLOCK_SIZE), indexBlock.blockSize
	}
	// key within first entries

	if bytes.Compare(indexBlock.minKey, key) <= 0 && bytes.Compare(key, indexBlock.metadataEntries[0].key) < 0 {
		startOffset := int64(index * MAX_BLOCK_SIZE)
		size := indexBlock.metadataEntries[0].offset
		return startOffset, size
	}
	// key within last entries
	lastIndexEntry := indexBlock.metadataEntries[len(indexBlock.metadataEntries)-1]
	if bytes.Compare(lastIndexEntry.key, key) <= 0 && bytes.Compare(key, indexBlock.maxKey) <= 0 {
		startOffset := int64(lastIndexEntry.offset) + int64(index*MAX_BLOCK_SIZE)
		size := indexBlock.blockSize - lastIndexEntry.offset
		return startOffset, size
	}

	low := 1
	high := len(indexBlock.metadataEntries) - 1
	for low <= high {
		mid := (low + high) / 2
		midIndex := indexBlock.metadataEntries[mid]
		beforeMid := indexBlock.metadataEntries[mid-1]

		if bytes.Compare(beforeMid.key, key) <= 0 && bytes.Compare(key, midIndex.key) < 0 {
			return int64(beforeMid.offset) + int64(MAX_BLOCK_SIZE*index), midIndex.offset - beforeMid.offset
		} else if bytes.Compare(key, midIndex.key) < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	// this is will not happen
	return int64(index * MAX_BLOCK_SIZE), indexBlock.blockSize
}

// get which data block have target key by searching its indexblock min and max
func (st *sstable) searchBlock(key []byte) int {
	low := 0
	high := len(st.indexBlocks) - 1

	for low <= high {
		mid := (low + high) / 2
		midIndex := st.indexBlocks[mid]
		if bytes.Compare(midIndex.minKey, key) <= 0 && bytes.Compare(key, midIndex.maxKey) <= 0 {
			return mid
		} else if bytes.Compare(key, midIndex.minKey) < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return -1
}

func (st *sstable) searchSstable(key []byte) (*memtable.Entry, error) {
	index := st.searchBlock(key)
	if index == -1 {
		return nil, nil
	}
	it, err := st.newIterator()
	if err != nil {
		return nil, err
	}
	defer it.close()

	startOffset, size := st.searchIndex(index, key)
	return it.seekAndSearchKey(key, startOffset, size)
}

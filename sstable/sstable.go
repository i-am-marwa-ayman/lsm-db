package sstable

import (
	"bytes"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
	"github.com/i-am-marwa-ayman/lsm-db/shared"
)

type sstable struct {
	fileName    string
	indexBlocks []*indexBlock
	size        int
	cfg         *shared.Config
	it          *iterator
}

func (sm *SsManager) newSstable(fileName string) *sstable {
	return &sstable{
		fileName:    fileName,
		indexBlocks: make([]*indexBlock, 0),
		cfg:         sm.cfg,
		it:          nil,
	}
}
func (st *sstable) writeSstable(entries []*memtable.Entry) error {
	w, err := st.newBlockWriter()
	if err != nil {
		return err
	}
	defer w.close()

	st.size = len(entries)
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
		return int64(index * int(st.cfg.MAX_IN_DISK_PAGE_SIZE)), indexBlock.blockSize
	}
	// key within first entries

	if bytes.Compare(indexBlock.minKey, key) <= 0 && bytes.Compare(key, indexBlock.metadataEntries[0].key) < 0 {
		startOffset := int64(index * int(st.cfg.MAX_IN_DISK_PAGE_SIZE))
		size := indexBlock.metadataEntries[0].offset
		return startOffset, size
	}
	// key within last entries
	lastIndexEntry := indexBlock.metadataEntries[len(indexBlock.metadataEntries)-1]
	if bytes.Compare(lastIndexEntry.key, key) <= 0 && bytes.Compare(key, indexBlock.maxKey) <= 0 {
		startOffset := int64(lastIndexEntry.offset) + int64(index*int(st.cfg.MAX_IN_DISK_PAGE_SIZE))
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
			return int64(beforeMid.offset) + int64(index*int(st.cfg.MAX_IN_DISK_PAGE_SIZE)), midIndex.offset - beforeMid.offset
		} else if bytes.Compare(key, midIndex.key) < 0 {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	// this is will not happen
	return int64(index * int(st.cfg.MAX_IN_DISK_PAGE_SIZE)), indexBlock.blockSize
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
	startOffset, size := st.searchIndex(index, key)
	return st.it.seekAndSearchKey(key, startOffset, size)
}
func (st *sstable) recover() error {
	var err error
	st.it, err = st.newIterator()
	if err != nil {
		return err
	}
	blockIndexOffsets, err := st.it.restoreFooter()
	if err != nil {
		return err
	}
	st.indexBlocks = make([]*indexBlock, len(blockIndexOffsets))
	for i, blockIndexOffset := range blockIndexOffsets {
		st.indexBlocks[i], err = st.it.restorIndexBlock(blockIndexOffset)
		if err != nil {
			return err
		}
		st.size += int(st.indexBlocks[i].blockEntriesCount)
	}
	return nil
}

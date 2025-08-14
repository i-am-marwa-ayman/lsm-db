package sstable

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
)

type iterator struct {
	filePtr     *os.File
	entries     []*memtable.Entry
	indexBlocks []*indexBlock
	curEntry    int
	curIndex    int
}

func (st *sstable) newIterator(indexBlocks []*indexBlock) (*iterator, error) {
	file, err := os.Open(st.fileName)
	if err != nil {
		return nil, err
	}
	return &iterator{
		filePtr:     file,
		entries:     nil,
		indexBlocks: indexBlocks,
		curEntry:    0,
		curIndex:    0,
	}, nil
}
func (it *iterator) decodeBlock(data []byte) error {
	index := it.indexBlocks[it.curIndex]
	buf := bytes.NewBuffer(data[:index.blockSize])
	it.entries = make([]*memtable.Entry, len(index.metadataEntries))
	for i := 0; i < len(index.metadataEntries); i++ {
		var keyLen int64
		err := binary.Read(buf, binary.LittleEndian, &keyLen)
		if err != nil {
			return err
		}
		key := make([]byte, keyLen)
		err = binary.Read(buf, binary.LittleEndian, key)
		if err != nil {
			return err
		}
		var valLen int64

		err = binary.Read(buf, binary.LittleEndian, &valLen)
		if err != nil {
			return err
		}
		val := make([]byte, valLen)
		err = binary.Read(buf, binary.LittleEndian, val)
		if err != nil {
			return err
		}
		var time int64
		err = binary.Read(buf, binary.LittleEndian, &time)
		if err != nil {
			return err
		}
		var deleted bool
		err = binary.Read(buf, binary.LittleEndian, &deleted)
		if err != nil {
			return err
		}
		entry := &memtable.Entry{
			Key:       string(key),
			Value:     string(val),
			Timestamp: time,
			Tombstone: deleted,
		}
		it.entries[i] = entry
	}
	return nil
}
func (it *iterator) load() error {
	data := make([]byte, MAX_BLOCK_SIZE)
	err := binary.Read(it.filePtr, binary.LittleEndian, data)
	if err != nil {
		return err
	}
	err = it.decodeBlock(data)
	if err != nil {
		return err
	}
	it.curIndex++
	it.curEntry = 0
	return nil
}

func (it *iterator) valid() (bool, error) {
	if it.curEntry == len(it.entries) {
		if it.curIndex == len(it.indexBlocks) {
			return false, nil
		} else {
			err := it.load()
			if err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (it *iterator) next() *memtable.Entry {
	entry := it.entries[it.curEntry]
	it.curEntry++
	return entry
}

func (it *iterator) close() {
	it.filePtr.Close()
}

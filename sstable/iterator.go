package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
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

func (st *sstable) newIterator() (*iterator, error) {
	file, err := os.Open(st.fileName)
	if err != nil {
		return nil, err
	}
	return &iterator{
		filePtr:     file,
		entries:     nil,
		indexBlocks: st.indexBlocks,
		curEntry:    0,
		curIndex:    0,
	}, nil
}
func (it *iterator) decodeEntry(buf *bytes.Buffer) (*memtable.Entry, error) {
	var keyLen int64
	err := binary.Read(buf, binary.LittleEndian, &keyLen)
	if err != nil {
		return nil, err
	}
	key := make([]byte, keyLen)
	err = binary.Read(buf, binary.LittleEndian, key)
	if err != nil {
		return nil, err
	}
	var valLen int64

	err = binary.Read(buf, binary.LittleEndian, &valLen)
	if err != nil {
		return nil, err
	}
	val := make([]byte, valLen)
	err = binary.Read(buf, binary.LittleEndian, val)
	if err != nil {
		return nil, err
	}
	var time int64
	err = binary.Read(buf, binary.LittleEndian, &time)
	if err != nil {
		return nil, err
	}
	var deleted bool
	err = binary.Read(buf, binary.LittleEndian, &deleted)
	if err != nil {
		return nil, err
	}
	return &memtable.Entry{
		Key:       string(key),
		Value:     string(val),
		Timestamp: time,
		Tombstone: deleted,
	}, nil
}
func (it *iterator) decodeBlock(data []byte) error {
	index := it.indexBlocks[it.curIndex]
	buf := bytes.NewBuffer(data[:index.blockSize])
	it.entries = make([]*memtable.Entry, index.blockEntriesCount)
	for i := 0; i < int(index.blockEntriesCount); i++ {
		entry, err := it.decodeEntry(buf)
		if err != nil {
			return err
		}
		it.entries[i] = entry
	}
	return nil
}
func (it *iterator) seekAndSearchKey(target string, start int64, size int32) (*memtable.Entry, error) {
	_, err := it.filePtr.Seek(start, io.SeekStart)
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	err = binary.Read(it.filePtr, binary.LittleEndian, data)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	for buf.Len() != 0 {
		entry, err := it.decodeEntry(buf)
		if err != nil {
			return nil, err
		}
		if entry.Key == target {
			return entry, nil
		}
	}
	return nil, nil
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

func (it *iterator) next() (*memtable.Entry, error) {
	if it.curEntry == len(it.entries) {
		if it.curIndex == len(it.indexBlocks) {
			return nil, nil // if there is no return nil
		}
		err := it.load()
		if err != nil {
			return nil, err
		}
	}
	entry := it.entries[it.curEntry]
	it.curEntry++
	return entry, nil
}

func (it *iterator) close() {
	it.filePtr.Close()
}

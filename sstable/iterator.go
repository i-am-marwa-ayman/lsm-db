package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
	"github.com/i-am-marwa-ayman/lsm-db/shared"
)

type iterator struct {
	filePtr     *os.File
	entries     []*memtable.Entry
	indexBlocks []*indexBlock
	curEntry    int
	curIndex    int
	cfg         *shared.Config
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
		cfg:         st.cfg,
	}, nil
}

// to read data from the start
func (it *iterator) seekStart() error {
	_, err := it.filePtr.Seek(0, io.SeekStart)
	it.entries = nil
	it.curEntry = 0
	it.curIndex = 0
	return err
}
func (it *iterator) restorIndexBlock(offset int64) (*indexBlock, error) {
	_, err := it.filePtr.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	indexBlock := &indexBlock{}
	err = binary.Read(it.filePtr, binary.LittleEndian, &indexBlock.blockEntriesCount)
	if err != nil {
		return nil, err
	}
	err = binary.Read(it.filePtr, binary.LittleEndian, &indexBlock.blockSize)
	if err != nil {
		return nil, err
	}
	var minKeyLen int64
	err = binary.Read(it.filePtr, binary.LittleEndian, &minKeyLen)
	if err != nil {
		return nil, err
	}
	indexBlock.minKey = make([]byte, minKeyLen)
	err = binary.Read(it.filePtr, binary.LittleEndian, &indexBlock.minKey)
	if err != nil {
		return nil, err
	}
	var maxKeyLen int64
	err = binary.Read(it.filePtr, binary.LittleEndian, &maxKeyLen)
	if err != nil {
		return nil, err
	}
	indexBlock.maxKey = make([]byte, maxKeyLen)
	err = binary.Read(it.filePtr, binary.LittleEndian, &indexBlock.maxKey)
	if err != nil {
		return nil, err
	}
	indexEntryNum := int(indexBlock.blockEntriesCount / it.cfg.SPARSE_INDEX_INTERVAL)
	indexBlock.metadataEntries = make([]*indexEntry, indexEntryNum)
	for i := range indexBlock.metadataEntries {
		iEntry := &indexEntry{}
		err = binary.Read(it.filePtr, binary.LittleEndian, &iEntry.offset)
		if err != nil {
			return nil, err
		}
		var keyLen int64
		err := binary.Read(it.filePtr, binary.LittleEndian, &keyLen)
		if err != nil {
			return nil, err
		}
		iEntry.key = make([]byte, keyLen)
		err = binary.Read(it.filePtr, binary.LittleEndian, iEntry.key)
		if err != nil {
			return nil, err
		}
		indexBlock.metadataEntries[i] = iEntry
	}
	return indexBlock, nil
}
func (it *iterator) restoreFooter() ([]int64, error) {
	fi, err := it.filePtr.Stat()
	if err != nil {
		return nil, err
	}
	_, err = it.filePtr.Seek(fi.Size()-8, io.SeekStart)
	if err != nil {
		return nil, err
	}
	var offsetNum int64
	err = binary.Read(it.filePtr, binary.LittleEndian, &offsetNum)
	if err != nil {
		return nil, err
	}
	temp := make([]byte, offsetNum*8)
	_, err = it.filePtr.Seek(fi.Size()-(offsetNum+1)*8, io.SeekStart)
	if err != nil {
		return nil, err
	}
	err = binary.Read(it.filePtr, binary.LittleEndian, temp)
	if err != nil {
		return nil, err
	}
	offsetBytes := bytes.NewBuffer(temp)
	offsets := make([]int64, offsetNum)
	for i := range offsets {
		err = binary.Read(offsetBytes, binary.LittleEndian, &offsets[i])
		if err != nil {
			return nil, err
		}
	}
	return offsets, nil
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
		Key:       key,
		Value:     val,
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
func (it *iterator) seekAndSearchKey(target []byte, start int64, size int32) (*memtable.Entry, error) {
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
		if bytes.Equal(entry.Key, target) {
			return entry, nil
		}
	}
	return nil, nil
}
func (it *iterator) load() error {
	data := make([]byte, it.cfg.MAX_IN_DISK_PAGE_SIZE)
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

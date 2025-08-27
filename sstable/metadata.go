package sstable

import (
	"bytes"
	"encoding/binary"
)

type indexEntry struct {
	offset int32 // offset within the block
	key    []byte
}

type indexBlock struct {
	blockSize         int32 // will not exceed 4 * 1024
	blockEntriesCount int32
	metadataEntries   []*indexEntry
	minKey            []byte
	maxKey            []byte
}

func (w *blockWriter) newIndexBlock() *indexBlock {
	return &indexBlock{
		blockSize:         0,
		blockEntriesCount: 0,
		metadataEntries:   make([]*indexEntry, 0),
		minKey:            make([]byte, 0),
		maxKey:            make([]byte, 0),
	}
}
func (b *indexBlock) addIndexEntry(offset int32, key []byte) {
	entry := &indexEntry{offset: offset, key: key}
	b.metadataEntries = append(b.metadataEntries, entry)
}
func (b *indexBlock) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	// entry count within block
	err := binary.Write(buf, binary.LittleEndian, b.blockEntriesCount)
	if err != nil {
		return nil, err
	}
	// data block actual size
	err = binary.Write(buf, binary.LittleEndian, b.blockSize)
	if err != nil {
		return nil, err
	}
	// minkey
	err = binary.Write(buf, binary.LittleEndian, int64(len(b.minKey)))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, b.minKey)
	if err != nil {
		return nil, err
	}
	// maxkey
	err = binary.Write(buf, binary.LittleEndian, int64(len(b.maxKey)))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, b.maxKey)
	if err != nil {
		return nil, err
	}
	for _, indexEntry := range b.metadataEntries {
		err := binary.Write(buf, binary.LittleEndian, indexEntry.offset)
		if err != nil {
			return nil, err
		}
		key := []byte(indexEntry.key)
		err = binary.Write(buf, binary.LittleEndian, int64(len(key)))
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.LittleEndian, key)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

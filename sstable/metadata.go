package sstable

import (
	"bytes"
	"encoding/binary"
)

type indexEntry struct {
	offset int16 // offset within the block
	key    string
}

type indexBlock struct {
	blockSize       int16 // will not exceed 4 * 1024
	metadataEntries []*indexEntry
}

func (w *blockWriter) newIndexBlock() *indexBlock {
	return &indexBlock{
		blockSize:       0,
		metadataEntries: make([]*indexEntry, 0),
	}
}
func (b *indexBlock) addIndexEntry(offset int, key string) {
	entry := &indexEntry{offset: int16(offset), key: key}
	b.metadataEntries = append(b.metadataEntries, entry)
}
func (b *indexBlock) toBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	// entry count within block
	err := binary.Write(buf, binary.LittleEndian, int64(len(b.metadataEntries)))
	if err != nil {
		return nil, err
	}
	// data block actual size
	err = binary.Write(buf, binary.LittleEndian, b.blockSize)
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

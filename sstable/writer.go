package sstable

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
)

const (
	MAX_BLOCK_SIZE = 4 * 1024
)

type blockWriter struct {
	filePtr        *os.File
	data           []byte
	indexBlocks    []*indexBlock
	metadataOffset []int64 // for the footer
	curIndex       *indexBlock
}

func (st *sstable) newBlockWriter() (*blockWriter, error) {
	file, err := os.OpenFile(st.fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	return &blockWriter{
		filePtr:        file,
		data:           make([]byte, 0),
		indexBlocks:    make([]*indexBlock, 0),
		metadataOffset: make([]int64, 0),
		curIndex:       nil,
	}, err
}

func (w *blockWriter) addEntry(entry *memtable.Entry) error {
	if w.curIndex == nil {
		w.curIndex = w.newIndexBlock()
	}

	rawEntry, err := entry.ToBytes()
	if err != nil {
		return err
	}
	if len(rawEntry)+len(w.data) <= MAX_BLOCK_SIZE {
		if w.curIndex.blockEntriesCount%10 == 0 {
			if w.curIndex.blockEntriesCount == 0 {
				w.curIndex.minKey = entry.Key
			} else {
				w.curIndex.addIndexEntry(int32(len(w.data)), entry.Key)
			}
		}
		w.curIndex.maxKey = entry.Key // always update maxkey
		w.data = append(w.data, rawEntry...)
		w.curIndex.blockEntriesCount++
	} else {
		err = w.flushDataBlock()
		if err != nil {
			return err
		}
		w.curIndex = w.newIndexBlock()
		w.addEntry(entry)
	}
	return nil
}
func (w *blockWriter) flushDataBlock() error {
	if len(w.data) == 0 {
		return nil
	}
	w.curIndex.blockSize = int32(len(w.data))
	w.indexBlocks = append(w.indexBlocks, w.curIndex)
	// fill data block
	if len(w.data) < MAX_BLOCK_SIZE {
		padding := make([]byte, MAX_BLOCK_SIZE-len(w.data))
		w.data = append(w.data, padding...)
	}
	// write 4k data block
	err := binary.Write(w.filePtr, binary.LittleEndian, w.data)
	if err != nil {
		return err
	}
	w.data = make([]byte, 0)
	return nil
}

func (w *blockWriter) writeIndexBlock(metadataBlock *indexBlock) error {
	offset, err := w.filePtr.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	rawMetadata, err := metadataBlock.toBytes()
	if err != nil {
		return err
	}
	err = binary.Write(w.filePtr, binary.LittleEndian, rawMetadata)
	if err != nil {
		return err
	}
	w.metadataOffset = append(w.metadataOffset, offset)
	return nil
}
func (w *blockWriter) writeFooter() error {
	for _, offset := range w.metadataOffset {
		err := binary.Write(w.filePtr, binary.LittleEndian, offset)
		if err != nil {
			return err
		}
	}
	err := binary.Write(w.filePtr, binary.LittleEndian, int64(len(w.metadataOffset)))
	return err
}
func (w *blockWriter) flushMetadataBlocks() error {
	for _, metadataBlock := range w.indexBlocks {
		err := w.writeIndexBlock(metadataBlock)
		if err != nil {
			return err
		}
	}
	err := w.writeFooter()
	return err
}
func (w *blockWriter) close() {
	w.filePtr.Close()
}

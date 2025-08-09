package sstable

import (
	"bytes"
	"encoding/binary"
	"github.com/i-am-marwa-ayman/lsm-db/memtable"
	"io"
	"os"
)

type stReader struct {
	filePtr *os.File
}

func (st *sstable) newReader() (*stReader, error) {
	file, err := os.Open(st.fileName)
	return &stReader{
		filePtr: file,
	}, err
}

// will be used in recover i guess???
//
//	func (r *stReader) readMetaData() (int, error) {
//		var n int64
//		err := binary.Read(r.filePtr, binary.LittleEndian, &n)
//		if err != nil {
//			return -1, err
//		}
//		return int(n), nil
//	}
func (r *stReader) seekToOffset(offset int64) error {
	_, err := r.filePtr.Seek(offset, io.SeekStart)
	return err
}

// pass offset position
func (r *stReader) readEntryAtOffset(offsetPos int64) (*memtable.Entry, error) {
	err := r.seekToOffset(offsetPos)
	if err != nil {
		return nil, err
	}
	entryOffset, err := r.readEntryOffset()
	if err != nil {
		return nil, err
	}
	err = r.seekToOffset(entryOffset)
	if err != nil {
		return nil, err
	}
	return r.readEntry()
}
func (r *stReader) readEntryOffset() (int64, error) {
	var entryOffset int64
	err := binary.Read(r.filePtr, binary.LittleEndian, &entryOffset)
	if err != nil {
		return 0, err
	}
	return entryOffset, nil
}
func (r *stReader) readEntry() (*memtable.Entry, error) {
	var err error
	var entrySize int64
	err = binary.Read(r.filePtr, binary.LittleEndian, &entrySize)
	if err != nil {
		return nil, err
	}
	entryBytes := make([]byte, entrySize)
	err = binary.Read(r.filePtr, binary.LittleEndian, entryBytes)
	if err != nil {
		return nil, err
	}
	entry := memtable.ToEntry(bytes.NewBuffer(entryBytes))
	return entry, nil
}
func (r *stReader) closeReader() {
	r.filePtr.Close()
}

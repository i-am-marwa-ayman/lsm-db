package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"mini-levelDB/memtable"
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
func (r *stReader) next() (*memtable.Entry, error) {
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

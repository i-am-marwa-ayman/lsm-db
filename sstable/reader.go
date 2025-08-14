package sstable

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
)

type blockReader struct {
	filePtr *os.File
}

func (st *sstable) newBlockReader() (*blockReader, error) {
	file, err := os.Open(st.fileName)
	return &blockReader{
		filePtr: file,
	}, err
}
func (r *blockReader) readEntryAtOffest(offset int64) (*memtable.Entry, error) {
	_, err := r.filePtr.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var keyLen int64
	err = binary.Read(r.filePtr, binary.LittleEndian, &keyLen)
	if err != nil {
		return nil, err
	}

	key := make([]byte, keyLen)
	err = binary.Read(r.filePtr, binary.LittleEndian, key)
	if err != nil {
		return nil, err
	}

	var valLen int64
	err = binary.Read(r.filePtr, binary.LittleEndian, &valLen)
	if err != nil {
		return nil, err
	}

	val := make([]byte, valLen)
	err = binary.Read(r.filePtr, binary.LittleEndian, val)
	if err != nil {
		return nil, err
	}

	var time int64
	err = binary.Read(r.filePtr, binary.LittleEndian, &time)
	if err != nil {
		return nil, err
	}

	var deleted bool
	err = binary.Read(r.filePtr, binary.LittleEndian, &deleted)
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
func (r *blockReader) close() {
	r.filePtr.Close()
}

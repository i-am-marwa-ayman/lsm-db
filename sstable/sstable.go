package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"mini-levelDB/memtable"
	"os"
)

//  ------------------------------------------------------------------------
// | num of entries                                                         |
//  ------------------------------------------------------------------------
// | size of entry | size of val | val | size of key | key | time | deleted |
//  -----------------------------------------------------------------------

// ff work in metadata
// type sstableMetaData struct {
// 	size int64
// }

// ff work in index
type sstable struct {
	fileName string
	filePtr  *os.File
	size     int
	// index    map[string]int64
}

func newSstable(fileName string) *sstable {
	return &sstable{
		fileName: fileName,
		filePtr:  nil,
		size:     0,
	}
}

func (st *sstable) writeHeader(size int) error {
	err := binary.Write(st.filePtr, binary.LittleEndian, int64(size))
	return err
}
func (st *sstable) readNextBlock() (*memtable.Entry, error) {
	var err error
	var entrySize int64
	err = binary.Read(st.filePtr, binary.LittleEndian, &entrySize)
	if err != nil {
		return nil, err
	}
	if entrySize < 0 || entrySize > 1024 {
		return nil, fmt.Errorf("invalid entry size")
	}
	entryBytes := make([]byte, entrySize)
	err = binary.Read(st.filePtr, binary.LittleEndian, entryBytes)
	if err != nil {
		return nil, err
	}
	entry := memtable.ToEntry(bytes.NewBuffer(entryBytes))
	return entry, nil
}
func (st *sstable) writeNextBlock(entry *memtable.Entry) error {
	buf, err := entry.ToBytes()
	if err != nil {
		return err
	}
	err = binary.Write(st.filePtr, binary.LittleEndian, int64(len(buf)))
	if err != nil {
		return err
	}
	_, err = st.filePtr.Write(buf)
	if err != nil {
		return err
	}
	return nil
}
func (st *sstable) writeData(allEntries []*memtable.Entry) error {
	var err error
	st.filePtr, err = os.OpenFile(st.fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)

	if err != nil {
		return err
	}
	defer st.filePtr.Close()
	err = binary.Write(st.filePtr, binary.LittleEndian, int64(len(allEntries)))
	if err != nil {
		return err
	}
	for _, entry := range allEntries {
		buf, err := entry.ToBytes()
		if err != nil {
			return err
		}
		err = binary.Write(st.filePtr, binary.LittleEndian, int64(len(buf)))
		if err != nil {
			return err
		}
		_, err = st.filePtr.Write(buf)
		if err != nil {
			return err
		}
	}
	st.size = len(allEntries)
	return nil
}

func (st *sstable) get(key string) (*memtable.Entry, error) {
	var err error
	st.filePtr, err = os.Open(st.fileName)
	if err != nil {
		return nil, err
	}
	defer st.filePtr.Close()
	var n int64
	err = binary.Read(st.filePtr, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(n); i++ {
		var entrySize int64
		err = binary.Read(st.filePtr, binary.LittleEndian, &entrySize)
		if err != nil {
			return nil, err
		}
		entryBytes := make([]byte, entrySize)
		err = binary.Read(st.filePtr, binary.LittleEndian, entryBytes)
		if err != nil {
			return nil, err
		}
		entry := memtable.ToEntry(bytes.NewBuffer(entryBytes))
		//fmt.Printf("key: %s, val: %s\n", entry.Key, entry.Value)
		if entry.Key == key {
			return entry, nil
		}
	}
	return nil, fmt.Errorf("key is not found")
}

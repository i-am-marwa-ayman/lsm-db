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

type sstable struct {
	fileName   string
	fileReader *os.File
	fileWriter *os.File
}

func newSstable(fileName string) *sstable {
	return &sstable{
		fileName:   fileName,
		fileReader: nil,
		fileWriter: nil,
	}
}

func (st *sstable) writeData(allEntries []*memtable.Entry) error {
	var err error
	st.fileWriter, err = os.Create(fmt.Sprintf(("../../data/%s"), st.fileName))
	if err != nil {
		return err
	}
	defer st.fileWriter.Close()
	err = binary.Write(st.fileWriter, binary.LittleEndian, int64(len(allEntries)))
	if err != nil {
		return err
	}
	for _, entry := range allEntries {
		buf, err := entry.ToBytes()
		if err != nil {
			continue
		}
		err = binary.Write(st.fileWriter, binary.LittleEndian, int64(len(buf)))
		if err != nil {
			continue
		}
		_, err = st.fileWriter.Write(buf)
		if err != nil {
			continue
		}
	}
	return nil
}

func (st *sstable) get(key string) (*memtable.Entry, error) {
	var err error
	st.fileReader, err = os.Open(fmt.Sprintf(("../../data/%s"), st.fileName))
	if err != nil {
		return nil, err
	}
	defer st.fileReader.Close()
	var n int64
	err = binary.Read(st.fileReader, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(n); i++ {
		var entrySize int64
		err = binary.Read(st.fileReader, binary.LittleEndian, &entrySize)
		if err != nil {
			return nil, err
		}
		entryBytes := make([]byte, entrySize)
		err = binary.Read(st.fileReader, binary.LittleEndian, entryBytes)
		if err != nil {
			return nil, err
		}
		entry := memtable.ToEntry(bytes.NewBuffer(entryBytes))
		if entry.Key == key {
			return entry, nil
		}
	}
	return nil, fmt.Errorf("key is not found")
}

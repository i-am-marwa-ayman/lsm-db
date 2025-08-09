package sstable

import (
	"fmt"
	"github.com/i-am-marwa-ayman/lsm-db/memtable"
	"io"
)

type sstable struct {
	fileName     string
	offsetsStart int64
	size         int64
}

func newSstable(fileName string) *sstable {
	return &sstable{
		fileName:     fileName,
		offsetsStart: 0,
		size:         0,
	}
}

func (st *sstable) writeSstable(entries []*memtable.Entry) error {
	w, err := st.newWriter()
	if err != nil {
		return err
	}
	defer w.closeWriter()

	st.size = int64(len(entries))

	offsets, err := w.writeData(entries)
	if err != nil {
		return err
	}
	st.offsetsStart, err = w.writeOffests(offsets)
	fmt.Println(st.offsetsStart)
	if err != nil {
		return err
	}
	err = w.writeMetaData(st.size, st.offsetsStart)
	if err != nil {
		return err
	}
	return nil
}
func (st *sstable) loadSstable() ([]*memtable.Entry, error) {
	r, err := st.newReader()
	if err != nil {
		return nil, err
	}
	defer r.closeReader()

	entries := make([]*memtable.Entry, 0)
	for i := 0; i < int(st.size); i++ {
		entry, err := r.readEntry()
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// for test
func (st *sstable) readSstable() error {
	r, err := st.newReader()
	if err != nil {
		return err
	}
	defer r.closeReader()

	for i := 0; i < int(st.size); i++ {
		offset, err := r.filePtr.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		entry, err := r.readEntry()
		if err != nil {
			return err
		}
		fmt.Printf("key: %s, val: %s, deleted: %t, offset : %d\n", entry.Key, entry.Value, entry.Tombstone, offset)
	}
	return nil
}

func (st *sstable) searchSstable(key string) (*memtable.Entry, error) {
	r, err := st.newReader()
	if err != nil {
		return nil, err
	}
	defer r.closeReader()

	low := 0
	high := int(st.size) - 1
	for low <= high {
		mid := low + (high-low)/2
		midOffset := int(st.offsetsStart) + mid*8
		// fmt.Printf("index: %d, offset Pos:%d\n", mid, midOffset)
		entry, err := r.readEntryAtOffset(int64(midOffset))
		if err != nil {
			return nil, err
		}

		if entry.Key == key {
			return entry, nil
		} else if entry.Key < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return nil, nil
}

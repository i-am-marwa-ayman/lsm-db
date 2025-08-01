package sstable

import (
	"fmt"
	"mini-levelDB/memtable"
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
	size     int
	// index    map[string]int64
}

func newSstable(fileName string) *sstable {
	return &sstable{
		fileName: fileName,
		size:     0,
	}
}

func (st *sstable) writeSstable(entries []*memtable.Entry) error {
	w, err := st.newWriter()
	if err != nil {
		return err
	}
	defer w.closeWriter()

	err = w.writeMetaData(len(entries))
	if err != nil {
		return err
	}

	for _, entry := range entries {
		err = w.writeNext(entry)
		if err != nil {
			return err
		}
	}
	st.size = len(entries)
	return nil
}

// for test
func (st *sstable) readSstable() error {
	r, err := st.newReader()
	if err != nil {
		return err
	}
	defer r.closeReader()

	fmt.Printf("size of sstable: %d\n", st.size)
	err = r.seekToOffset(8)
	if err != nil {
		return err
	}

	for i := 0; i < st.size; i++ {
		entry, err := r.next()
		if err != nil {
			return err
		}
		fmt.Printf("key: %s, val: %s\n", entry.Key, entry.Value)
	}
	return nil
}

func (st *sstable) searchSstable(key string) (*memtable.Entry, error) {
	r, err := st.newReader()
	if err != nil {
		return nil, err
	}
	defer r.closeReader()

	err = r.seekToOffset(8)
	if err != nil {
		return nil, err
	}

	for i := 0; i < st.size; i++ {
		entry, err := r.next()
		if err != nil {
			return nil, err
		}
		//fmt.Printf("key: %s, val: %s\n", entry.Key, entry.Value)
		if entry.Key == key {
			return entry, nil
		}
	}
	return nil, nil
}

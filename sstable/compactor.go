package sstable

import (
	"mini-levelDB/memtable"
)

// TODO: use seek
func (st *sstable) Compact(first *sstable, second *sstable) error {
	rFirst, err := first.newReader()
	if err != nil {
		return err
	}
	defer rFirst.closeReader()
	err = rFirst.seekToOffset(8)
	if err != nil {
		return err
	}
	rSecond, err := second.newReader()
	if err != nil {
		return err
	}
	defer rSecond.closeReader()
	err = rSecond.seekToOffset(8)
	if err != nil {
		return err
	}
	w, err := st.newWriter()
	if err != nil {
		return err
	}
	defer w.closeWriter()

	err = w.writeMetaData(0)
	if err != nil {
		return err
	}
	ptr1 := 0
	ptr2 := 0
	var entry1 *memtable.Entry
	entry1, err = rFirst.next()
	if err != nil {
		return err
	}
	var entry2 *memtable.Entry
	entry2, err = rSecond.next()
	if err != nil {
		return err
	}
	newSize := 0
	for ptr1 < first.size && ptr2 < second.size {
		if entry1.Key == entry2.Key {
			if !entry2.Tombstone {
				err = w.writeNext(entry2)
				if err != nil {
					return err
				}
				newSize++
			}
			ptr1++
			if ptr1 < first.size {
				entry1, err = rFirst.next()
				if err != nil {
					return err
				}
			}
			ptr2++
			if ptr2 < second.size {
				entry2, err = rSecond.next()
				if err != nil {
					return err
				}
			}
		} else if entry1.Key < entry2.Key {
			if !entry1.Tombstone {
				err = w.writeNext(entry1)
				if err != nil {
					return err
				}
				newSize++
			}
			ptr1++
			if ptr1 < first.size {
				entry1, err = rFirst.next()
				if err != nil {
					return err
				}
			}
		} else {
			if !entry2.Tombstone {
				err = w.writeNext(entry2)
				if err != nil {
					return err
				}
				newSize++
			}
			ptr2++
			if ptr2 < second.size {
				entry2, err = rSecond.next()
				if err != nil {
					return err
				}
			}
		}
	}
	for ptr1 < first.size {
		if !entry1.Tombstone {
			err = w.writeNext(entry1)
			if err != nil {
				return err
			}
			newSize++
		}
		ptr1++
		if ptr1 < first.size {
			entry1, err = rFirst.next()
			if err != nil {
				return err
			}
		}
	}
	for ptr2 < second.size {
		if !entry2.Tombstone {
			err = w.writeNext(entry2)
			if err != nil {
				return err
			}
			newSize++
		}
		ptr2++
		if ptr2 < second.size {
			entry2, err = rSecond.next()
			if err != nil {
				return err
			}
		}
	}
	err = w.seekToOffset(0)
	if err != nil {
		return err
	}
	st.size = newSize
	err = w.writeMetaData(st.size)
	return err
}

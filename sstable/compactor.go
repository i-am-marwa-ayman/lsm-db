package sstable

import (
	"encoding/binary"
	"io"
	"mini-levelDB/memtable"
	"os"
)

func close(first *sstable, second *sstable, newsst *sstable) {
	first.filePtr.Close()
	second.filePtr.Close()
	newsst.filePtr.Close()
}

// TODO: use seek
// TODO: use defer
func (st *sstable) Compact(first *sstable, second *sstable) error {
	var err error
	first.filePtr, err = os.Open(first.fileName)
	if err != nil {
		return err
	}
	second.filePtr, err = os.Open(second.fileName)
	if err != nil {
		return err
	}
	st.filePtr, err = os.OpenFile(st.fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer close(first, second, st)
	err = st.writeHeader(0)
	if err != nil {
		return err
	}
	var n int64
	err = binary.Read(first.filePtr, binary.LittleEndian, &n)
	if err != nil {
		return err
	}
	var nn int64
	err = binary.Read(second.filePtr, binary.LittleEndian, &nn)
	if err != nil {
		return err
	}
	ptr1 := 0
	ptr2 := 0
	var entry1 *memtable.Entry
	entry1, err = first.readNextBlock()
	if err != nil {
		return err
	}
	var entry2 *memtable.Entry
	entry2, err = second.readNextBlock()
	if err != nil {
		return err
	}
	newSize := 0
	for ptr1 < int(n) && ptr2 < int(nn) {
		if entry1.Key == entry2.Key {
			if !entry2.Tombstone {
				err = st.writeNextBlock(entry2)
				if err != nil {
					return err
				}
				newSize++
			}
			entry1, err = first.readNextBlock()
			if err != nil && err != io.EOF {
				return err
			}
			entry2, err = second.readNextBlock()
			if err != nil && err != io.EOF {
				return err
			}
			ptr1++
			ptr2++
		} else if entry1.Key < entry2.Key {
			if !entry1.Tombstone {
				st.writeNextBlock(entry1)
				newSize++
			}
			entry1, err = first.readNextBlock()
			if err != nil && err != io.EOF {
				return err
			}
			ptr1++
		} else {
			if !entry2.Tombstone {
				st.writeNextBlock(entry2)
				newSize++
			}
			entry2, err = second.readNextBlock()
			if err != nil && err != io.EOF {
				return err
			}
			ptr2++
		}
	}
	for ptr1 < first.size {
		if !entry1.Tombstone {
			st.writeNextBlock(entry1)
			newSize++
		}
		entry1, err = first.readNextBlock()
		if err != nil && err != io.EOF {
			return err
		}
		ptr1++
	}
	for ptr2 < second.size {
		if !entry2.Tombstone {
			st.writeNextBlock(entry2)
			newSize++
		}
		entry2, err = second.readNextBlock()
		if err != nil && err != io.EOF {
			return err
		}
		ptr2++
	}
	_, err = st.filePtr.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	st.size = newSize
	err = st.writeHeader(newSize)
	return err
}

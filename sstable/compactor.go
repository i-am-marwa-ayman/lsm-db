package sstable

import (
	"github.com/i-am-marwa-ayman/lsm-db/memtable"
)

// need refactor not readable at all
func (st *sstable) Compact(first *sstable, second *sstable) error {
	firstReader, err := first.newReader()
	if err != nil {
		return err
	}
	defer firstReader.closeReader()
	secondReader, err := second.newReader()
	if err != nil {
		return err
	}
	defer secondReader.closeReader()

	w, err := st.newWriter()
	if err != nil {
		return err
	}
	defer w.closeWriter()

	firstIndx := 0
	secondIndx := 0
	var currentFirstEntry *memtable.Entry
	currentFirstEntry, err = firstReader.readEntry()
	if err != nil {
		return err
	}
	var currentSecondEntry *memtable.Entry
	currentSecondEntry, err = secondReader.readEntry()
	if err != nil {
		return err
	}

	newSize := 0
	newOffsets := make([]int64, 0)

	for firstIndx < int(first.size) && secondIndx < int(second.size) {
		if currentFirstEntry.Key == currentSecondEntry.Key {
			if !currentSecondEntry.Tombstone {
				offset, err := w.writeEntry(currentSecondEntry)
				newOffsets = append(newOffsets, offset)
				if err != nil {
					return err
				}
				newSize++
			}
			firstIndx++
			if firstIndx < int(first.size) {
				currentFirstEntry, err = firstReader.readEntry()
				if err != nil {
					return err
				}
			}
			secondIndx++
			if secondIndx < int(second.size) {
				currentSecondEntry, err = secondReader.readEntry()
				if err != nil {
					return err
				}
			}
		} else if currentFirstEntry.Key < currentSecondEntry.Key {
			if !currentFirstEntry.Tombstone {
				offset, err := w.writeEntry(currentFirstEntry)
				newOffsets = append(newOffsets, offset)
				if err != nil {
					return err
				}
				newSize++
			}
			firstIndx++
			if firstIndx < int(first.size) {
				currentFirstEntry, err = firstReader.readEntry()
				if err != nil {
					return err
				}
			}
		} else {
			if !currentSecondEntry.Tombstone {
				offset, err := w.writeEntry(currentSecondEntry)
				newOffsets = append(newOffsets, offset)
				if err != nil {
					return err
				}
				newSize++
			}
			secondIndx++
			if secondIndx < int(second.size) {
				currentSecondEntry, err = secondReader.readEntry()
				if err != nil {
					return err
				}
			}
		}
	}
	for firstIndx < int(first.size) {
		if !currentFirstEntry.Tombstone {
			offset, err := w.writeEntry(currentFirstEntry)
			newOffsets = append(newOffsets, offset)
			if err != nil {
				return err
			}
			newSize++
		}
		firstIndx++
		if firstIndx < int(first.size) {
			currentFirstEntry, err = firstReader.readEntry()
			if err != nil {
				return err
			}
		}
	}

	for secondIndx < int(second.size) {
		if !currentSecondEntry.Tombstone {
			offset, err := w.writeEntry(currentSecondEntry)
			newOffsets = append(newOffsets, offset)
			if err != nil {
				return err
			}
			newSize++
		}
		secondIndx++
		if secondIndx < int(second.size) {
			currentSecondEntry, err = secondReader.readEntry()
			if err != nil {
				return err
			}
		}
	}
	st.size = int64(newSize)
	st.offsetsStart, err = w.writeOffests(newOffsets)
	if err != nil {
		return err
	}

	err = w.writeMetaData(st.size, st.offsetsStart)

	return err
}

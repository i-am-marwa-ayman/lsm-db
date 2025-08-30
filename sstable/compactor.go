package sstable

import (
	"bytes"
)

func (st *sstable) compact(first *sstable, second *sstable, deleteZombie bool) error {
	firstIterator := first.it
	defer firstIterator.close()

	err := firstIterator.seekStart()
	if err != nil {
		return err
	}

	secondIterator := second.it
	defer secondIterator.close()

	err = secondIterator.seekStart()
	if err != nil {
		return err
	}

	w, err := st.newBlockWriter()
	if err != nil {
		return err
	}
	defer w.close()

	currentFirstEntry, err := firstIterator.next()
	if err != nil {
		return err
	}
	currentSecondEntry, err := secondIterator.next()
	if err != nil {
		return err
	}

	for currentFirstEntry != nil && currentSecondEntry != nil {
		if bytes.Equal(currentFirstEntry.Key, currentSecondEntry.Key) {
			if !(currentSecondEntry.Tombstone && deleteZombie) {
				err = w.addEntry(currentSecondEntry)
				if err != nil {
					return err
				}
				st.size++
			}
			currentFirstEntry, err = firstIterator.next()
			if err != nil {
				return err
			}
			currentSecondEntry, err = secondIterator.next()
			if err != nil {
				return err
			}
		} else if bytes.Compare(currentFirstEntry.Key, currentSecondEntry.Key) < 0 {
			if !(currentFirstEntry.Tombstone && deleteZombie) {
				err = w.addEntry(currentFirstEntry)
				if err != nil {
					return err
				}
				st.size++
			}
			currentFirstEntry, err = firstIterator.next()
			if err != nil {
				return err
			}
		} else {
			if !(currentSecondEntry.Tombstone && deleteZombie) {
				err = w.addEntry(currentSecondEntry)
				if err != nil {
					return err
				}
				st.size++
			}
			currentSecondEntry, err = secondIterator.next()
			if err != nil {
				return err
			}
		}
	}
	for currentFirstEntry != nil {
		if !(currentFirstEntry.Tombstone && deleteZombie) {
			err = w.addEntry(currentFirstEntry)
			if err != nil {
				return err
			}
			st.size++
		}
		currentFirstEntry, err = firstIterator.next()
		if err != nil {
			return err
		}
	}
	for currentSecondEntry != nil {
		if !(currentSecondEntry.Tombstone && deleteZombie) {
			err = w.addEntry(currentSecondEntry)
			if err != nil {
				return err
			}
			st.size++
		}
		currentSecondEntry, err = secondIterator.next()
		if err != nil {
			return err
		}
	}

	err = w.flushDataBlock()
	if err != nil {
		return err
	}
	err = w.flushMetadataBlocks()
	if err != nil {
		return err
	}
	st.indexBlocks = w.indexBlocks
	return nil
}

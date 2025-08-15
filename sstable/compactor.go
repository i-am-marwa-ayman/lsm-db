package sstable

func (st *sstable) compact(first *sstable, second *sstable, deleteZombie bool) error {
	firstIterator, err := first.newIterator()
	if err != nil {
		return err
	}
	defer firstIterator.close()
	secondIterator, err := second.newIterator()
	if err != nil {
		return err
	}
	defer secondIterator.close()

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
		if currentFirstEntry.Key == currentSecondEntry.Key {
			if !(currentSecondEntry.Tombstone && deleteZombie) {
				err = w.addEntry(currentSecondEntry)
				if err != nil {
					return err
				}
			}
			currentFirstEntry, err = firstIterator.next()
			if err != nil {
				return err
			}
			currentSecondEntry, err = secondIterator.next()
			if err != nil {
				return err
			}
		} else if currentFirstEntry.Key < currentSecondEntry.Key {
			if !(currentFirstEntry.Tombstone && deleteZombie) {
				err = w.addEntry(currentFirstEntry)
				if err != nil {
					return err
				}
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

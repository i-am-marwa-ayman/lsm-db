package sstable

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
)

type stWriter struct {
	filePtr *os.File
}

func (st *sstable) newWriter() (*stWriter, error) {
	file, err := os.OpenFile(st.fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	return &stWriter{
		filePtr: file,
	}, err
}

func (w *stWriter) seekToOffset(offset int64) error {
	_, err := w.filePtr.Seek(offset, io.SeekStart)
	return err
}
func (w *stWriter) writeMetaData(size int64, offsetPos int64) error {
	err := binary.Write(w.filePtr, binary.LittleEndian, size)
	if err != nil {
		return err
	}
	err = binary.Write(w.filePtr, binary.LittleEndian, offsetPos)
	return err
}
func (w *stWriter) writeOffests(Offests []int64) (int64, error) {
	offsetsPos, err := w.filePtr.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	for _, offset := range Offests {
		err := binary.Write(w.filePtr, binary.LittleEndian, int64(offset))
		if err != nil {
			return 0, err
		}
	}
	return offsetsPos, nil
}
func (w *stWriter) writeData(entries []*memtable.Entry) ([]int64, error) {
	var err error = nil
	offsets := make([]int64, len(entries))
	for i, entry := range entries {
		offsets[i], err = w.writeEntry(entry)
		if err != nil {
			return nil, err
		}
	}
	return offsets, nil
}
func (w *stWriter) writeEntry(entry *memtable.Entry) (int64, error) {
	offset, err := w.filePtr.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	buf, err := entry.ToBytes()
	if err != nil {
		return 0, err
	}
	err = binary.Write(w.filePtr, binary.LittleEndian, int64(len(buf)))
	if err != nil {
		return 0, err
	}
	_, err = w.filePtr.Write(buf)
	if err != nil {
		return 0, err
	}
	return offset, nil
}
func (w *stWriter) closeWriter() {
	w.filePtr.Close()
}

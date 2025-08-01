package sstable

import (
	"encoding/binary"
	"io"
	"mini-levelDB/memtable"
	"os"
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

func (r *stWriter) seekToOffset(offset int64) error {
	_, err := r.filePtr.Seek(offset, io.SeekStart)
	return err
}
func (r *stWriter) writeMetaData(size int) error {
	err := binary.Write(r.filePtr, binary.LittleEndian, int64(size))
	return err
}
func (r *stWriter) writeNext(entry *memtable.Entry) error {
	buf, err := entry.ToBytes()
	if err != nil {
		return err
	}
	err = binary.Write(r.filePtr, binary.LittleEndian, int64(len(buf)))
	if err != nil {
		return err
	}
	_, err = r.filePtr.Write(buf)
	if err != nil {
		return err
	}
	return nil
}
func (r *stWriter) closeWriter() {
	r.filePtr.Close()
}

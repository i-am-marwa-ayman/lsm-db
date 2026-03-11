package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/i-am-marwa-ayman/lsm-db/shared"
)

type Wal struct {
	cfg     *shared.Config
	filePtr *os.File
	lock    *sync.Mutex
}

func NewWal(cfg *shared.Config) (*Wal, error) {
	file, err := os.OpenFile(cfg.DATA_PATH+"/wal.log", os.O_CREATE|os.O_RDWR, 0644)
	return &Wal{
		cfg:     cfg,
		filePtr: file,
		lock:    &sync.Mutex{},
	}, err
}
func (wal *Wal) Recover() ([]*shared.Entry, error) {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	_, err := wal.filePtr.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	info, err := wal.filePtr.Stat()
	if err != nil {
		return nil, err
	}
	data := make([]byte, info.Size())

	err = binary.Read(wal.filePtr, binary.LittleEndian, data)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	enties := make([]*shared.Entry, 0)
	for buf.Len() != 0 {
		var keyLen int64
		err = binary.Read(buf, binary.LittleEndian, &keyLen)
		if err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		err = binary.Read(buf, binary.LittleEndian, key)
		if err != nil {
			return nil, err
		}
		var valLen int64

		err = binary.Read(buf, binary.LittleEndian, &valLen)
		if err != nil {
			return nil, err
		}
		val := make([]byte, valLen)
		err = binary.Read(buf, binary.LittleEndian, val)
		if err != nil {
			return nil, err
		}
		var time int64
		err = binary.Read(buf, binary.LittleEndian, &time)
		if err != nil {
			return nil, err
		}
		var deleted bool
		err = binary.Read(buf, binary.LittleEndian, &deleted)
		if err != nil {
			return nil, err
		}
		entry := &shared.Entry{
			Key:       key,
			Value:     val,
			Timestamp: time,
			Tombstone: deleted,
		}
		enties = append(enties, entry)
	}
	return enties, nil
}
func (wal *Wal) Append(entry *shared.Entry) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	entryBytes, err := entry.ToBytes()
	if err != nil {
		return err
	}
	err = binary.Write(wal.filePtr, binary.LittleEndian, entryBytes)
	return err
}

func (wal *Wal) Clear() error {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	err := wal.filePtr.Truncate(0)
	if err != nil {
		return err
	}
	_, err = wal.filePtr.Seek(0, 0)
	return err
}
func (wal *Wal) Close() {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	wal.filePtr.Close()
}

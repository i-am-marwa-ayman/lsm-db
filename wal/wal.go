package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"sync"

	"github.com/i-am-marwa-ayman/lsm-db/shared"
)

type Wal struct {
	cfg         *shared.Config
	filePtr     *os.File
	lock        *sync.Mutex
	segments    []int64
	curSegSize  int64
}

func NewWal(cfg *shared.Config) (*Wal, error) {
	file, err := os.OpenFile(cfg.DATA_PATH+"/wal.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	return &Wal{
		cfg:         cfg,
		filePtr:     file,
		lock:        &sync.Mutex{},
		segments:    make([]int64, 0),
		curSegSize:  0,
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
	log.Printf("[Wal] WAL file size: %d bytes\n", info.Size())
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

		wal.curSegSize += int64(entry.Size())
		if wal.curSegSize >= wal.cfg.MAX_IN_MEMORY_SIZE {
			log.Printf("[Wal] Current WAL segment size %d bytes reached limit, starting new segment\n", wal.curSegSize)
			wal.segments = append(wal.segments, wal.curSegSize)
			wal.curSegSize = 0
		}
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
	if err != nil {
		return err
	}
	entrySize := int64(len(entryBytes))
	wal.curSegSize += entrySize
	if wal.curSegSize >= wal.cfg.MAX_IN_MEMORY_SIZE {
		log.Printf("[Wal] Current WAL segment size %d bytes reached limit, starting new segment\n", wal.curSegSize)
		wal.segments = append(wal.segments, wal.curSegSize)
		wal.curSegSize = 0
	}
	if wal.cfg.SYNC {
		err = wal.filePtr.Sync()
		if err != nil {
			return err
		}
	}
	return nil
}
func (wal *Wal) ClearSegment() error {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	if len(wal.segments) == 0 {
		return nil
	}
	segmentsToClear := wal.segments[0]
	wal.segments = wal.segments[1:]
	_, err := wal.filePtr.Seek(segmentsToClear, io.SeekStart)
	if err != nil {
		return err
	}

	tempFile, err := os.OpenFile(wal.cfg.DATA_PATH+"/temp.log", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = io.Copy(tempFile, wal.filePtr)
	if err != nil {
		return err
	}
	if wal.cfg.SYNC {
		err = tempFile.Sync()
		if err != nil {
			return err
		}
	}
	err = wal.filePtr.Close()
	if err != nil {
		return err
	}
	err = tempFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(wal.cfg.DATA_PATH+"/temp.log", wal.cfg.DATA_PATH+"/wal.log")
	if err != nil {
		return err
	}
	wal.filePtr, err = os.OpenFile(wal.cfg.DATA_PATH+"/wal.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	// new file size
	info, err := wal.filePtr.Stat()
	if err != nil {
		return err
	}
	log.Printf("[Wal] Cleared %d bytes from WAL, new WAL size is %d bytes\n", segmentsToClear, info.Size())
	return nil
}
func (wal *Wal) Close() {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	wal.filePtr.Close()
}

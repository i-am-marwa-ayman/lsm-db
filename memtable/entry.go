package memtable

import (
	"bytes"
	"encoding/binary"
	"time"
)

type Entry struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Timestamp int64
}

func NewEntry(key []byte, val []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     val,
		Tombstone: false,
		Timestamp: int64(time.Now().Unix()),
	}
}
func DeletedEntry(key []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     make([]byte, 0),
		Tombstone: true,
		Timestamp: int64(time.Now().Unix()),
	}
}
func (entry *Entry) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int64(len(entry.Key)))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, entry.Key)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, int64(len(entry.Value)))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, entry.Value)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, int64(entry.Timestamp))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, entry.Tombstone)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

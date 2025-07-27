package memtable

import (
	"bytes"
	"encoding/binary"
	"time"
)

type Entry struct {
	Key       string
	Value     string
	Tombstone bool
	Timestamp int64
}

func NewEntry(key string, val string) *Entry {
	return &Entry{
		Key:       key,
		Value:     val,
		Tombstone: false,
		Timestamp: int64(time.Now().Unix()),
	}
}
func ToEntry(buf *bytes.Buffer) *Entry {
	var keyLen int64
	err := binary.Read(buf, binary.LittleEndian, &keyLen)
	if err != nil {
		return nil
	}
	key := make([]byte, keyLen)
	err = binary.Read(buf, binary.LittleEndian, key)
	if err != nil {
		return nil
	}
	var valLen int64

	err = binary.Read(buf, binary.LittleEndian, &valLen)
	if err != nil {
		return nil
	}
	val := make([]byte, valLen)
	err = binary.Read(buf, binary.LittleEndian, val)
	if err != nil {
		return nil
	}
	var time int64
	err = binary.Read(buf, binary.LittleEndian, &time)
	if err != nil {
		return nil
	}
	var deleted bool
	err = binary.Read(buf, binary.LittleEndian, &deleted)
	if err != nil {
		return nil
	}
	return &Entry{
		Key:       string(key),
		Value:     string(val),
		Timestamp: time,
		Tombstone: deleted,
	}
}
func DeletedEntry(key string) *Entry {
	return &Entry{
		Key:       key,
		Value:     "",
		Tombstone: true,
		Timestamp: int64(time.Now().Unix()),
	}
}
func (entry *Entry) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	key := []byte(entry.Key)
	err := binary.Write(buf, binary.LittleEndian, int64(len(key)))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, key)
	if err != nil {
		return nil, err
	}
	val := []byte(entry.Value)
	err = binary.Write(buf, binary.LittleEndian, int64(len(val)))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, val)
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

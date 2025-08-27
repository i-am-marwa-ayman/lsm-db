package engine

import (
	"fmt"
	"log"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
	"github.com/i-am-marwa-ayman/lsm-db/sstable"
)

type Engine struct {
	memtable       *memtable.MemTable
	sstableManager *sstable.SsManager
}

func NewEngine(DataPath string) (*Engine, error) {
	sstableManager, err := sstable.NewSsManager(DataPath)
	if err != nil {
		return nil, err
	}
	return &Engine{
		memtable:       memtable.NewMemtable(),
		sstableManager: sstableManager,
	}, nil
}

func (db *Engine) Get(key string) (string, error) {
	entry := db.memtable.Get([]byte(key))
	if entry != nil {
		if !entry.Tombstone {
			return string(entry.Value), nil
		} else {
			return "", fmt.Errorf("key does not exist")
		}
	}

	entry = db.sstableManager.Get([]byte(key))
	if entry != nil && !entry.Tombstone {
		return string(entry.Value), nil
	}

	return "", fmt.Errorf("key does not exist")
}

func (db *Engine) Set(key string, val string) error {
	db.memtable.Set([]byte(key), []byte(val))
	log.Printf("key %s is inserted\n", key)
	if db.memtable.IsFull() {
		log.Println("full table")
		log.Println("loading to disk...")
		err := db.sstableManager.AddSstable(db.memtable.GetAll())
		if err != nil {
			return err
		}
		db.memtable = memtable.NewMemtable()
	}
	return nil
}

func (db *Engine) Delete(key string) error {
	db.memtable.Delete([]byte(key))
	log.Printf("key %s is deleted\n", key)
	if db.memtable.IsFull() {
		log.Println("full table")
		log.Println("loading to disk...")
		err := db.sstableManager.AddSstable(db.memtable.GetAll())
		if err != nil {
			return err
		}
		db.memtable = memtable.NewMemtable()
	}
	return nil
}

package engine

import (
	"fmt"
	"log"
	"sync"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
	"github.com/i-am-marwa-ayman/lsm-db/shared"
	"github.com/i-am-marwa-ayman/lsm-db/sstable"
)

type Engine struct {
	memtable       *memtable.MemTable
	sstableManager *sstable.SsManager
	cfg            *shared.Config
	lock           *sync.Mutex
}

func NewEngine(cfg *shared.Config) (*Engine, error) {
	sstableManager, err := sstable.NewSsManager(cfg)
	if err != nil {
		return nil, err
	}
	return &Engine{
		memtable:       memtable.NewMemtable(cfg),
		sstableManager: sstableManager,
		cfg:            cfg,
		lock:           &sync.Mutex{},
	}, nil
}
func (db *Engine) Close() {
	db.sstableManager.Close()
}
func (db *Engine) Get(key string) (string, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
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
	db.lock.Lock()
	defer db.lock.Unlock()
	err := db.memtable.Set([]byte(key), []byte(val))
	if err != nil {
		return err
	}
	log.Printf("key %s is inserted\n", key)
	if db.memtable.IsFull() {
		log.Println("full table")
		log.Println("loading to disk...")
		err := db.sstableManager.AddSstable(db.memtable.GetAll())
		if err != nil {
			return err
		}
		db.memtable = memtable.NewMemtable(db.cfg)
	}
	return nil
}

func (db *Engine) Delete(key string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	err := db.memtable.Delete([]byte(key))
	if err != nil {
		return err
	}
	log.Printf("key %s is deleted\n", key)
	if db.memtable.IsFull() {
		log.Println("full table")
		log.Println("loading to disk...")
		err = db.sstableManager.AddSstable(db.memtable.GetAll())
		fmt.Println(err)
		if err != nil {
			return err
		}
		db.memtable = memtable.NewMemtable(db.cfg)
	}
	return nil
}

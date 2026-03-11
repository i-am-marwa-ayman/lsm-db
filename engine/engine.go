package engine

import (
	"fmt"
	"log"
	"sync"

	"github.com/i-am-marwa-ayman/lsm-db/memtable"
	"github.com/i-am-marwa-ayman/lsm-db/shared"
	"github.com/i-am-marwa-ayman/lsm-db/sstable"
	"github.com/i-am-marwa-ayman/lsm-db/wal"
)

type Engine struct {
	memtable       *memtable.MemTable
	sstableManager *sstable.SsManager
	wal            *wal.Wal
	cfg            *shared.Config
	lock           *sync.Mutex
}

func NewEngine() (*Engine, error) {
	db := &Engine{
		cfg:  shared.NewConfig(),
		lock: &sync.Mutex{},
	}
	log.Printf("[Engine] Initializing LSM-DB with data path: %s\n", db.cfg.DATA_PATH)
	db.memtable = memtable.NewMemtable(db.cfg)

	var err error
	db.wal, err = wal.NewWal(db.cfg)
	entries, err := db.wal.Recover()

	for _, entry := range entries {
		err = db.memtable.AddEntry(entry)
		if err != nil {
			return nil, err
		}
	}
	log.Printf("[Engine] Memtable recovered with %d bytes from WAL\n", db.memtable.Size())

	db.sstableManager, err = sstable.NewSsManager(db.cfg)
	if err != nil {
		log.Printf("[Engine] Failed to initialize SSTable manager: %v\n", err)
		return nil, err
	}
	log.Println("[Engine] Successfully initialized LSM-DB engine")
	return db, nil
}
func (db *Engine) Flush() error {
	log.Printf("[Engine] Flushing memtable (%d bytes) to disk...\n", db.memtable.Size())
	err := db.sstableManager.AddSstable(db.memtable.GetAll())
	if err != nil {
		return err
	}
	err = db.wal.Clear()
	if err != nil {
		return err
	}
	db.memtable = memtable.NewMemtable(db.cfg)
	return nil
}
func (db *Engine) Close() error {
	db.wal.Close()
	return db.sstableManager.Close()
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
	entry := shared.NewEntry([]byte(key), []byte(val))
	err := db.wal.Append(entry)
	if err != nil {
		return err
	}
	err = db.memtable.Set([]byte(key), []byte(val))
	if err != nil {
		return err
	}
	log.Printf("[Engine] SET key=%s\n", key)
	if db.memtable.IsFull() {
		log.Println("[Engine] Memtable size limit reached, triggering flush")
		err = db.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Engine) Delete(key string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	entry := shared.DeletedEntry([]byte(key))
	err := db.wal.Append(entry)
	if err != nil {
		return err
	}
	err = db.memtable.Delete([]byte(key))
	if err != nil {
		return err
	}
	log.Printf("[Engine] DELETE key=%s\n", key)
	if db.memtable.IsFull() {
		log.Println("[Engine] Memtable size limit reached, triggering flush")
		err = db.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

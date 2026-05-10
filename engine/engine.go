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
	imutable       []*memtable.MemTable
	sstableManager *sstable.SsManager
	cfg            *shared.Config
	lock           *sync.RWMutex
	flushCh        chan struct{}
	wg             sync.WaitGroup
}

func NewEngine() (*Engine, error) {
	db := &Engine{
		cfg:  shared.NewConfig(),
		lock: &sync.RWMutex{},
	}
	log.Printf("[Engine] Initializing LSM-DB with data path: %s\n", db.cfg.DATA_PATH)

	db.memtable = memtable.NewMemtable(db.cfg)
	var err error
	db.sstableManager, err = sstable.NewSsManager(db.cfg)
	if err != nil {
		log.Printf("[Engine] Failed to initialize SSTable manager: %v\n", err)
		return nil, err
	}
	db.flushCh = make(chan struct{}, 10)
	db.wg.Add(1)
	go db.flushWorker()
	log.Println("[Engine] Successfully initialized LSM-DB engine")
	return db, nil
}

func (db *Engine) flushWorker() {
	defer db.wg.Done()
	for range db.flushCh {
		db.lock.Lock()
		toFlush := db.imutable[0]
		db.lock.Unlock()

		err := db.sstableManager.AddSstable(toFlush.GetAll())
		if err != nil {
			log.Printf("[Engine] error happend while flushing %v\n", err)
		}

		db.lock.Lock()
		db.imutable = db.imutable[1:]
		db.lock.Unlock()
	}
}
func (db *Engine) Close() error {
	db.lock.Lock()
	if !db.memtable.IsEmpty() {
		log.Println("[Engine] Flushing remaining memtable before shutdown")
		db.imutable = append(db.imutable, db.memtable)
		db.lock.Unlock()
		db.flushCh <- struct{}{}
	} else {
		db.lock.Unlock()
	}
	close(db.flushCh)
	db.wg.Wait()
	return db.sstableManager.Close()
}
func (db *Engine) Get(key string) (string, error) {
	entry := db.searchMemtable([]byte(key))

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

// we reduce the time of set by making the flush happen in the background
// it is not a finer lock but it is a good start to make the flush happen in the background without blocking the main thread
func (db *Engine) Set(key string, val string) error {
	db.lock.Lock()
	err := db.memtable.Set([]byte(key), []byte(val))
	if err != nil {
		return err
	}
	log.Printf("[Engine] SET key=%s\n", key)

	if db.memtable.IsFull() {
		log.Println("[Engine] Memtable size limit reached, triggering flush")
		db.imutable = append(db.imutable, db.memtable)
		db.memtable = memtable.NewMemtable(db.cfg)
		db.lock.Unlock()

		db.flushCh <- struct{}{}
	} else {
		db.lock.Unlock()
	}
	return nil
}

func (db *Engine) Delete(key string) error {
	db.lock.Lock()
	err := db.memtable.Delete([]byte(key))
	if err != nil {
		return err
	}
	log.Printf("[Engine] DELETE key=%s\n", key)

	if db.memtable.IsFull() {
		log.Println("[Engine] Memtable size limit reached, triggering flush")
		db.imutable = append(db.imutable, db.memtable)
		db.memtable = memtable.NewMemtable(db.cfg)
		db.lock.Unlock()

		db.flushCh <- struct{}{}
	} else {
		db.lock.Unlock()
	}
	return nil
}

func (db *Engine) searchMemtable(key []byte) *shared.Entry {
	db.lock.RLock()
	defer db.lock.RUnlock()

	entry := db.memtable.Get(key)
	if entry != nil {
		return entry
	}

	for i := len(db.imutable) - 1; i >= 0; i-- {
		entry = db.imutable[i].Get(key)
		if entry != nil {
			return entry
		}
	}
	return nil
}

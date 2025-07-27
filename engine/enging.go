package engine

import (
	"fmt"
	"log"
	"mini-levelDB/memtable"
	"mini-levelDB/sstable"
)

type Engine struct {
	memtable       *memtable.MemTable
	sstableManager sstable.SsManager
}

func NewEngine() *Engine {
	return &Engine{
		memtable:       memtable.NewMemtable(),
		sstableManager: *sstable.NewSsManager(),
	}
}

func (db *Engine) Get(key string) (string, error) { // i think i will make the memtable take a []byte
	entry := db.memtable.Get(key)
	if entry != nil && !entry.Tombstone {
		return entry.Value, nil
	}

	entry = db.sstableManager.Get(key)
	if entry != nil && !entry.Tombstone {
		return entry.Value, nil
	}

	return "", fmt.Errorf("key does not exist")
}

func (db *Engine) Set(key string, val string) {
	db.memtable.Set(key, val)
	log.Printf("key %s is inserted\n", key)
	if db.memtable.IsFull() {
		log.Println("full table")
		log.Println("loading to disk...")
		err := db.sstableManager.AddSstable(db.memtable.GetAll())
		if err != nil {
			log.Println("somthing worg happend!")
			log.Println(err)
		}
		db.memtable = memtable.NewMemtable()
	}
}

func (db *Engine) Delete(key string) {
	db.memtable.Delete(key)
	log.Printf("key %s is deleted\n", key)
	if db.memtable.IsFull() {
		log.Println("full table")
		log.Println("loading to disk...")
		err := db.sstableManager.AddSstable(db.memtable.GetAll())
		if err != nil {
			log.Println("somthing worg happend!")
			log.Println(err)
		}
		db.memtable = memtable.NewMemtable()
	}
}

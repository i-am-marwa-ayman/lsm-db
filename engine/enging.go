package engine

import "mini-levelDB/memtable"

type Engine struct {
	Memtable  *memtable.MemTable
	immutable []*memtable.MemTable
}

func NewEngine() *Engine {
	return &Engine{
		Memtable:  memtable.NewMemtable(),
		immutable: make([]*memtable.MemTable, 0),
	}
}

func (DB *Engine) Get(key string) string { // i think i will make the memtable take a []byte
	val := DB.Memtable.Get(key)
	return val.Value
}

func (DB *Engine) Set(key string, val string) {
	DB.Memtable.Set(key, val)
}

func (DB *Engine) Delete(key string) {
	DB.Memtable.Delete(key)
}

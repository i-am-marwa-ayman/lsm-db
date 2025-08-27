package memtable

type MemTable struct {
	root    *AVL
	size    int
	maxSize int
}

func NewMemtable() *MemTable {
	return &MemTable{
		root:    nil,
		size:    0,
		maxSize: 500, // will change just for test
	}
}
func (mt *MemTable) IsFull() bool {
	return mt.maxSize == mt.size
}
func (mt *MemTable) Get(key []byte) *Entry {
	entry := mt.root.LookUp(key)
	return entry
}
func (mt *MemTable) Set(key []byte, val []byte) {
	nEntry := NewEntry(key, val)
	newAdd := 0
	mt.root, newAdd = mt.root.Insert(key, nEntry)
	mt.size += newAdd // will add one if we update non-existing val in memtable
}
func (mt *MemTable) Delete(key []byte) {
	nEntry := DeletedEntry(key)
	newAdd := 0
	mt.root, newAdd = mt.root.Insert(key, nEntry)
	mt.size += newAdd
}
func (mt *MemTable) GetAll() []*Entry {
	return mt.root.GetAll()
}

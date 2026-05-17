package shared

type Config struct {
	MAX_IN_DISK_PAGE_SIZE int64
	MAX_IN_MEMORY_SIZE    int64
	SPARSE_INDEX_INTERVAL int64
	DATA_PATH             string
	ENABLE_WAL            bool
	SYNC                  bool
}

func NewConfig() *Config {
	return &Config{
		MAX_IN_DISK_PAGE_SIZE: 4 * 1024,
		MAX_IN_MEMORY_SIZE:    16 * 1024,
		SPARSE_INDEX_INTERVAL: 10,
		DATA_PATH:             "/home/marwa/study/cmu-db/lsm-db/data", // if you change this in config you need to change it in engine_test.go/main.go as well
		ENABLE_WAL:            true,
		SYNC:                  false,
	}
}

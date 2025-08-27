package shared

type Config struct {
	MAX_IN_DISK_PAGE_SIZE int32
	MAX_IN_MEMORY_SIZE    int32
	SPARSE_INDEX_INTERVAL int32
	DATA_PATH             string
}

func NewConfig() *Config {
	return &Config{
		MAX_IN_DISK_PAGE_SIZE: 4 * 1024,
		MAX_IN_MEMORY_SIZE:    16 * 1024,
		SPARSE_INDEX_INTERVAL: 10,
		DATA_PATH:             "./../data",
	}
}

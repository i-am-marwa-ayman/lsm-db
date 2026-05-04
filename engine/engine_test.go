package engine

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
)

func resetDataDir(b *testing.B) {
	b.Helper()
	dataPath := "/home/marwa/study/cmu-db/lsm-db/data"
	if err := os.RemoveAll(dataPath); err != nil {
		b.Fatalf("failed to remove data dir: %v", err)
	}
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		b.Fatalf("failed to recreate data dir: %v", err)
	}
}

func GetRandomNumber(size int) []int {
	randoms := make([]int, size)
	seen := make(map[int]bool, size)
	idx := 0

	for len(seen) < size {
		n := rand.IntN(size)
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = true
		randoms[idx] = n
		idx++
	}

	return randoms
}

func BenchmarkEngineRecover(b *testing.B) {
	resetDataDir(b)

	db, err := NewEngine()
	if err != nil {
		b.Fatalf("engine failed: %v", err)
	}

	inputs := GetRandomNumber(b.N)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", inputs[i])
		value := fmt.Sprintf("value%d", inputs[i])
		if err = db.Set(key, value); err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
	if err = db.Close(); err != nil {
		b.Fatalf("close failed: %v", err)
	}

	b.ResetTimer()

	db, err = NewEngine()
	if err != nil {
		b.Fatalf("engine failed: %v", err)
	}
	defer db.Close()

	search := GetRandomNumber(b.N)
	fail := 0
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", search[i])
		_, err := db.Get(key)
		if err != nil {
			fail++
			b.Logf("failed to get key: %s, error: %v", key, err)
			continue
		}
	}
	b.Logf("recover benchmark: keys found %d/%d", b.N-fail, b.N)
}

func BenchmarkEngineSet(b *testing.B) {
	resetDataDir(b)

	db, err := NewEngine()
	if err != nil {
		b.Fatalf("engine failed: %v", err)
		return
	}
	defer db.Close()

	inputs := GetRandomNumber(b.N)

	b.ReportAllocs()
	b.ResetTimer()

	numKeys := b.N
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", inputs[i])
		value := fmt.Sprintf("value%d", inputs[i])
		if err := db.Set(key, value); err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
}

func BenchmarkEngineGet(b *testing.B) {
	resetDataDir(b)

	db, err := NewEngine()
	if err != nil {
		b.Fatalf("engine failed: %v", err)
		return
	}
	defer db.Close()

	inputs := GetRandomNumber(b.N)
	search := GetRandomNumber(b.N)

	numKeys := b.N
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", inputs[i])
		value := fmt.Sprintf("value%d", inputs[i])
		if err := db.Set(key, value); err != nil {
			b.Fatalf("set failed: %v", err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", search[i])
		if _, err := db.Get(key); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}
func BenchmarkEngineUpdate(b *testing.B) {
	resetDataDir(b)

	db, err := NewEngine()
	if err != nil {
		b.Fatalf("engine failed: %v", err)
		return
	}
	defer db.Close()

	inputs := GetRandomNumber(b.N)
	update := GetRandomNumber(b.N)
	search := GetRandomNumber(b.N)

	b.ReportAllocs()
	b.ResetTimer()

	numKeys := b.N
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", inputs[i])
		value := fmt.Sprintf("value%d", inputs[i])
		if err = db.Set(key, value); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", update[i])
		value := fmt.Sprintf("new%d", update[i])
		if err = db.Set(key, value); err != nil {
			b.Fatalf("update failed: %v", err)
		}
	}
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", search[i])
		if val, err := db.Get(key); err != nil || val != fmt.Sprintf("new%d", search[i]) {
			b.Fatalf("get failed: %v", err)
		}
	}
}
func BenchmarkEngineDelete(b *testing.B) {
	resetDataDir(b)

	db, err := NewEngine()
	if err != nil {
		b.Fatalf("engine failed: %v", err)
		return
	}
	defer db.Close()

	inputs := GetRandomNumber(b.N)
	delete := GetRandomNumber(b.N)
	search := GetRandomNumber(b.N)

	b.ReportAllocs()
	b.ResetTimer()

	numKeys := b.N
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", inputs[i])
		value := fmt.Sprintf("value%d", inputs[i])
		if err = db.Set(key, value); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", delete[i])
		if err = db.Delete(key); err != nil {
			b.Fatalf("delete failed: %v", err)
		}
	}
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%d", search[i])
		if _, err := db.Get(key); err == nil {
			b.Fatalf("get deleted data failed: %v", err)
		}
	}
}

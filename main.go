package main

import (
	"fmt"
	"os"

	"github.com/i-am-marwa-ayman/lsm-db/engine"
)

func resetDataDir() error {
	dataPath := "/home/marwa/study/cmu-db/lsm-db/data"
	if err := os.RemoveAll(dataPath); err != nil {
		fmt.Printf("failed to remove data dir: %v", err)
		return err
	}
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		fmt.Printf("failed to recreate data dir: %v", err)
		return err
	}
	return nil
}

func main() {

	err := resetDataDir()
	if err != nil {
		fmt.Println("Failed to reset data directory:", err)
		return
	}

	db, err := engine.NewEngine()
	if err != nil {
		fmt.Println(err)
		return
	}
	// Test: Set and Get
	err = db.Set("marwa", "ayman")
	if err != nil {
		fmt.Println("Set failed:", err)
		return
	}
	err = db.Set("hello", "world")
	if err != nil {
		fmt.Println("Set failed:", err)
		return
	}

	val, err := db.Get("marwa")
	if err != nil {
		fmt.Println("Get failed:", err)
	} else {
		fmt.Println("Retrieved value:", val)
	}
	err = db.Close()
	if err != nil {
		fmt.Println("Close failed:", err)
	}
}

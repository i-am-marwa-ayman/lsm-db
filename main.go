package main

import (
	"fmt"
	"mini-levelDB/engine"
)

func main() {
	db := engine.NewEngine()
	db.Set("marwa", "ayman")
	db.Set("amira", "tarek")
	db.Set("yasmin", "ahmed")
	db.Set("dina", "mohamed")
	db.Set("safy", "ahmed")
	db.Set("mira", "ashraf")
	db.Set("marwa", "alwany")
	//db.Delete("marwa")
	fmt.Println(db.Get("marwa"))
	//db.Memtable.Root.PrintAll()
}

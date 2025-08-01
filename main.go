package main

import (
	"fmt"
	"mini-levelDB/engine"
)

func main() {
	db := engine.NewEngine()
	db.Set("language", "python")
	db.Set("framework", "django")
	db.Set("database", "postgres")
	db.Set("port", "8080")
	db.Set("environment", "production")
	db.Set("version", "1.0.0")
	db.Set("cache", "redis")
	db.Set("protocol", "https")
	db.Set("host", "localhost")
	db.Set("timeout", "30s")
	db.Set("user", "admin")

	db.Delete("language")
	db.Delete("framework")
	db.Delete("database")
	db.Delete("port")
	db.Delete("environment")
	db.Delete("version")
	db.Delete("cache")
	db.Delete("protocol")
	db.Delete("host")
	db.Delete("timeout")
	db.Delete("user")

	val, err := db.Get("user")
	if err == nil {
		fmt.Println(val)
	} else {
		fmt.Println(err)
	}
}

package main

import (
	"fmt"
	"github.com/i-am-marwa-ayman/lsm-db/engine"
)

func main() {
	db := engine.NewEngine()
	db.Set("language", "python")
	db.Set("framework", "django")
	db.Set("database", "postgresql")
	db.Set("version", "1.0.0")
	db.Set("status", "active")
	db.Set("theme", "dark")
	db.Set("mode", "production")
	db.Set("timeout", "30s")
	db.Set("max_connections", "100")
	db.Set("host", "localhost")
	db.Set("port", "8080")
	db.Set("cache_enabled", "true")
	db.Set("retry_attempts", "3")
	db.Set("log_level", "info")
	db.Set("username", "admin")
	db.Set("password", "secret")
	db.Set("email", "admin@example.com")
	db.Set("region", "us-east-1")
	db.Set("currency", "USD")
	db.Set("timezone", "UTC")
	db.Set("color", "blue")
	db.Set("font", "monospace")
	db.Set("compression", "gzip")
	db.Set("api_key", "1234567890")
	db.Set("ssl", "enabled")
	db.Set("backup", "daily")
	db.Set("autosave", "true")
	db.Set("language", "golang")
	db.Set("framework", "react")
	db.Set("build", "release")
	db.Set("os", "linux")
	db.Set("arch", "amd64")
	db.Set("container", "docker")
	db.Set("orchestrator", "kubernetes")
	db.Set("max_retries", "5")
	db.Set("session_timeout", "15m")
	db.Set("feature_flag", "beta")
	db.Set("payment_gateway", "stripe")
	db.Set("queue", "rabbitmq")
	db.Set("cdn", "cloudflare")

	val, err := db.Get("version")
	if err == nil {
		fmt.Println(val)
	} else {
		fmt.Println(err)
	}
}

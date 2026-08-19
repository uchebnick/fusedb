// Command example demonstrates the supported FuseDB library API.
package main

import (
	"fmt"
	"log"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	db, err := fusedb.Open(fusedb.Options{
		Dir:       "./example-data",
		CacheSize: 5 << 20,
		MergeSize: 5 << 20,
	})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close database: %w", closeErr)
		}
	}()

	fmt.Println("FuseDB example")

	users := []string{"Alice", "Bob", "Charlie"}
	for i, name := range users {
		key := []byte(fmt.Sprintf("user:%d:name", i+1))
		if err := db.Put(key, []byte(name)); err != nil {
			return fmt.Errorf("put %q: %w", key, err)
		}
	}

	for i := 1; i <= len(users); i++ {
		key := []byte(fmt.Sprintf("user:%d:name", i))
		value, found, err := db.Get(key)
		if err != nil {
			return fmt.Errorf("get %q: %w", key, err)
		}
		if found {
			fmt.Printf("%s = %s\n", key, value)
		}
	}

	if err := db.Inc([]byte("page:home:views"), 10); err != nil {
		return fmt.Errorf("increment views: %w", err)
	}
	if err := db.Inc([]byte("page:home:views"), 5); err != nil {
		return fmt.Errorf("increment views: %w", err)
	}
	views, found, err := db.GetInt64([]byte("page:home:views"))
	if err != nil {
		return fmt.Errorf("get views: %w", err)
	}
	if found {
		fmt.Printf("page:home:views = %d\n", views)
	}

	if err := db.Delete([]byte("user:2:name")); err != nil {
		return fmt.Errorf("delete user:2:name: %w", err)
	}
	_, found, err = db.Get([]byte("user:2:name"))
	if err != nil {
		return fmt.Errorf("verify delete: %w", err)
	}
	fmt.Printf("user:2:name found = %v\n", found)

	stats := db.Stats()
	fmt.Printf("buffered bytes = %d, leaves = %d\n", stats.BufferedBytes, stats.Leaves)
	return nil
}

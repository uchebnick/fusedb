package fusedb_test

import (
	"fmt"
	"log"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

func Example() {
	// Open database
	db, err := fusedb.Open(fusedb.Options{
		Dir:       "/tmp/example-db",
		CacheSize: 5 << 20, // 5MB
		MergeSize: 5 << 20, // 5MB
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Put a value
	if err := db.Put([]byte("user:1:name"), []byte("Alice")); err != nil {
		log.Fatal(err)
	}

	// Get a value
	value, found, err := db.Get([]byte("user:1:name"))
	if err != nil {
		log.Fatal(err)
	}
	if found {
		fmt.Printf("Name: %s\n", value)
	}

	// Increment a counter
	if err := db.Inc([]byte("user:1:visits"), 1); err != nil {
		log.Fatal(err)
	}

	// Delete a key
	if err := db.Delete([]byte("user:1:name")); err != nil {
		log.Fatal(err)
	}

	// Check stats
	stats := db.Stats()
	fmt.Printf("Buffered: %d bytes\n", stats.BufferedBytes)
}

func ExampleDB_Put() {
	db, _ := fusedb.Open(fusedb.Options{Dir: "/tmp/db"})
	defer func() { _ = db.Close() }()

	err := db.Put([]byte("key"), []byte("value"))
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleDB_Get() {
	db, _ := fusedb.Open(fusedb.Options{Dir: "/tmp/db"})
	defer func() { _ = db.Close() }()

	value, found, err := db.Get([]byte("key"))
	if err != nil {
		log.Fatal(err)
	}
	if found {
		fmt.Printf("Value: %s\n", value)
	} else {
		fmt.Println("Key not found")
	}
}

func ExampleDB_Inc() {
	db, _ := fusedb.Open(fusedb.Options{Dir: "/tmp/db"})
	defer func() { _ = db.Close() }()

	// Increment counter
	if err := db.Inc([]byte("page:views"), 1); err != nil {
		log.Fatal(err)
	}

	// Decrement counter
	if err := db.Inc([]byte("page:views"), -1); err != nil {
		log.Fatal(err)
	}
}

func ExampleDB_Delete() {
	db, _ := fusedb.Open(fusedb.Options{Dir: "/tmp/db"})
	defer func() { _ = db.Close() }()

	err := db.Delete([]byte("key"))
	if err != nil {
		log.Fatal(err)
	}
}

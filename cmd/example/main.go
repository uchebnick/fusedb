// Simple example of using FuseDB as a library
package main

import (
	"fmt"
	"log"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

func main() {
	// Open database
	db, err := fusedb.Open(fusedb.Options{
		Dir:       "./example-data",
		CacheSize: 5 << 20, // 5MB
		MergeSize: 5 << 20, // 5MB
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	fmt.Println("FuseDB Example")
	fmt.Println("==============")

	// Put some values
	fmt.Println("\n1. Storing values...")
	if err := db.Put([]byte("user:1:name"), []byte("Alice")); err != nil {
		log.Fatal(err)
	}
	if err := db.Put([]byte("user:2:name"), []byte("Bob")); err != nil {
		log.Fatal(err)
	}
	if err := db.Put([]byte("user:3:name"), []byte("Charlie")); err != nil {
		log.Fatal(err)
	}

	// Get values
	fmt.Println("\n2. Reading values...")
	for i := 1; i <= 3; i++ {
		key := []byte(fmt.Sprintf("user:%d:name", i))
		value, found, err := db.Get(key)
		if err != nil {
			log.Fatal(err)
		}
		if found {
			fmt.Printf("   %s = %s\n", key, value)
		}
	}

	// Increment counters
	fmt.Println("\n3. Using counters...")
	if err := db.Inc([]byte("page:home:views"), 10); err != nil {
		log.Fatal(err)
	}
	if err := db.Inc([]byte("page:home:views"), 5); err != nil {
		log.Fatal(err)
	}
	if err := db.Inc([]byte("page:about:views"), 3); err != nil {
		log.Fatal(err)
	}

	// Delete a key
	fmt.Println("\n4. Deleting a key...")
	if err := db.Delete([]byte("user:2:name")); err != nil {
		log.Fatal(err)
	}

	value, found, _ := db.Get([]byte("user:2:name"))
	if !found {
		fmt.Println("   user:2:name deleted successfully")
	} else {
		fmt.Printf("   ERROR: key still exists with value: %s\n", value)
	}

	// Show stats
	fmt.Println("\n5. Database stats...")
	stats := db.Stats()
	fmt.Printf("   Buffered: %d bytes\n", stats.BufferedBytes)

	fmt.Println("\nDone!")
}

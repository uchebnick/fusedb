package fusedb_test

import (
	"fmt"
	"os"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

func Example() {
	db, cleanup := openExampleDB()
	defer cleanup()

	if err := db.Put([]byte("user:1:name"), []byte("Alice")); err != nil {
		panic(err)
	}
	if err := db.Inc([]byte("user:1:visits"), 1); err != nil {
		panic(err)
	}

	name, found, err := db.Get([]byte("user:1:name"))
	if err != nil {
		panic(err)
	}
	visits, _, err := db.GetInt64([]byte("user:1:visits"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("name=%s found=%v visits=%d\n", name, found, visits)
	// Output: name=Alice found=true visits=1
}

func ExampleDB_Put() {
	db, cleanup := openExampleDB()
	defer cleanup()

	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		panic(err)
	}
}

func ExampleDB_Get() {
	db, cleanup := openExampleDB()
	defer cleanup()

	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		panic(err)
	}
	value, found, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("value=%s found=%v\n", value, found)
	// Output: value=value found=true
}

func ExampleDB_Inc() {
	db, cleanup := openExampleDB()
	defer cleanup()

	if err := db.Inc([]byte("page:views"), 2); err != nil {
		panic(err)
	}
	if err := db.Inc([]byte("page:views"), -1); err != nil {
		panic(err)
	}
	count, found, err := db.GetInt64([]byte("page:views"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("count=%d found=%v\n", count, found)
	// Output: count=1 found=true
}

func ExampleDB_Delete() {
	db, cleanup := openExampleDB()
	defer cleanup()

	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		panic(err)
	}
	if err := db.Delete([]byte("key")); err != nil {
		panic(err)
	}
	_, found, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("found=%v\n", found)
	// Output: found=false
}

func ExampleDB_ApplyOnce() {
	db, cleanup := openExampleDB()
	defer cleanup()

	mutations := []fusedb.Mutation{
		fusedb.PutMutation([]byte("purchase/order-991"), []byte("paid")),
		fusedb.IncMutation([]byte("tickets/show-55/sold"), 3),
	}
	first, err := db.ApplyOnce([]byte("event/order-991-paid"), mutations)
	if err != nil {
		panic(err)
	}
	retry, err := db.ApplyOnce([]byte("event/order-991-paid"), mutations)
	if err != nil {
		panic(err)
	}
	count, _, err := db.GetInt64([]byte("tickets/show-55/sold"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("first=%v retry=%v tickets=%d\n", first, retry, count)
	// Output: first=true retry=false tickets=3
}

func openExampleDB() (*fusedb.DB, func()) {
	dir, err := os.MkdirTemp("", "fusedb-example-")
	if err != nil {
		panic(err)
	}
	db, err := fusedb.Open(fusedb.Options{Dir: dir})
	if err != nil {
		_ = os.RemoveAll(dir)
		panic(err)
	}
	return db, func() {
		if err := db.Close(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}
}

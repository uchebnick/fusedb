package fusedb_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

func TestConditionalLikeAndCounterAreAtomicAndIdempotent(t *testing.T) {
	db, err := fusedb.Open(fusedb.DurablePilotOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	likeKey := []byte("like/post-42/user-7")
	countKey := []byte("likes/post-42")
	apply := func() (bool, error) {
		return db.Apply(
			[]fusedb.Condition{fusedb.KeyAbsent(likeKey)},
			[]fusedb.Mutation{
				fusedb.PutMutation(likeKey, []byte("2026-08-19T18:00:00Z")),
				fusedb.IncMutation(countKey, 1),
			},
		)
	}
	if applied, err := apply(); err != nil || !applied {
		t.Fatalf("first apply = (%v, %v)", applied, err)
	}
	if applied, err := apply(); err != nil || applied {
		t.Fatalf("duplicate apply = (%v, %v)", applied, err)
	}
	count, found, err := db.GetInt64(countKey)
	if err != nil || !found || count != 1 {
		t.Fatalf("count = (%d, %v, %v), want 1", count, found, err)
	}
}

func TestApplyOnceConcurrentPurchaseDelivery(t *testing.T) {
	db, err := fusedb.Open(fusedb.DurablePilotOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	eventKey := []byte("event/purchase/order-991")
	ticketKey := []byte("tickets/event-55/sold")
	orderKey := []byte("purchase/order-991")
	mutations := []fusedb.Mutation{
		fusedb.PutMutation(orderKey, []byte("paid")),
		fusedb.IncMutation(ticketKey, 3),
	}

	var applied atomic.Int64
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 64)
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			committed, err := db.ApplyOnce(eventKey, mutations)
			if err != nil {
				errorsSeen <- err
				return
			}
			if committed {
				applied.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("apply once: %v", err)
	}
	if got := applied.Load(); got != 1 {
		t.Fatalf("applied calls = %d, want 1", got)
	}
	count, found, err := db.GetInt64(ticketKey)
	if err != nil || !found || count != 3 {
		t.Fatalf("ticket count = (%d, %v, %v), want 3", count, found, err)
	}
	status, found, err := db.Get(orderKey)
	if err != nil || !found || string(status) != "paid" {
		t.Fatalf("order = (%q, %v, %v)", status, found, err)
	}

	conflicting := []fusedb.Mutation{
		fusedb.PutMutation(orderKey, []byte("paid")),
		fusedb.IncMutation(ticketKey, 4),
	}
	if committed, err := db.ApplyOnce(eventKey, conflicting); committed || !errors.Is(err, fusedb.ErrIdempotencyConflict) {
		t.Fatalf("conflicting reuse = (%v, %v)", committed, err)
	}
}

func TestApplyOnceSurvivesMergeCloseAndReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	options := fusedb.DurablePilotOptions(dir)
	options.MergeSize = 8 << 10
	options.MaxLeafSize = 32 << 10
	db, err := fusedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}

	const purchases = 500
	for index := range purchases {
		applied, err := db.ApplyOnce(
			[]byte(fmt.Sprintf("event/purchase/%06d", index)),
			[]fusedb.Mutation{
				fusedb.PutMutation([]byte(fmt.Sprintf("purchase/%06d", index)), []byte("paid")),
				fusedb.IncMutation([]byte("tickets/sold"), 1),
			},
		)
		if err != nil || !applied {
			t.Fatalf("purchase %d = (%v, %v)", index, applied, err)
		}
	}
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := fusedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	count, found, err := reopened.GetInt64([]byte("tickets/sold"))
	if err != nil || !found || count != purchases {
		t.Fatalf("reopened count = (%d, %v, %v), want %d", count, found, err, purchases)
	}
	if applied, err := reopened.ApplyOnce(
		[]byte("event/purchase/000123"),
		[]fusedb.Mutation{
			fusedb.PutMutation([]byte("purchase/000123"), []byte("paid")),
			fusedb.IncMutation([]byte("tickets/sold"), 1),
		},
	); err != nil || applied {
		t.Fatalf("replayed event after reopen = (%v, %v)", applied, err)
	}
	if _, err := reopened.Verify(t.Context()); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestApplyOnceIfPersistsRejectedDecision(t *testing.T) {
	db, err := fusedb.Open(fusedb.DurablePilotOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	eventKey := []byte("event/unlike/post-9/user-4/request-1")
	likeKey := []byte("like/post-9/user-4")
	mutations := []fusedb.Mutation{
		fusedb.DeleteMutation(likeKey),
		fusedb.IncMutation([]byte("likes/post-9"), -1),
	}
	applied, err := db.ApplyOnceIf(eventKey, []fusedb.Condition{fusedb.KeyPresent(likeKey)}, mutations)
	if err != nil || applied {
		t.Fatalf("first rejected event = (%v, %v)", applied, err)
	}
	if err := db.Put(likeKey, []byte("later-like")); err != nil {
		t.Fatal(err)
	}
	applied, err = db.ApplyOnceIf(eventKey, []fusedb.Condition{fusedb.KeyPresent(likeKey)}, mutations)
	if err != nil || applied {
		t.Fatalf("delayed retry = (%v, %v)", applied, err)
	}
	if value, found, err := db.Get(likeKey); err != nil || !found || string(value) != "later-like" {
		t.Fatalf("newer like was changed: (%q, %v, %v)", value, found, err)
	}
	if _, found, err := db.GetInt64([]byte("likes/post-9")); err != nil || found {
		t.Fatalf("rejected unlike changed counter: (found=%v, err=%v)", found, err)
	}
}

func TestConcurrentDistinctPurchaseEventsRemainExactAfterReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	options := fusedb.DurablePilotOptions(dir)
	db, err := fusedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 8
	const perWorker = 100
	errorsSeen := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range perWorker {
				id := fmt.Sprintf("%02d-%04d", worker, index)
				applied, err := db.ApplyOnce(
					[]byte("event/purchase/"+id),
					[]fusedb.Mutation{
						fusedb.PutMutation([]byte("purchase/"+id), []byte("paid")),
						fusedb.IncMutation([]byte("tickets/show-1/sold"), 1),
					},
				)
				if err != nil || !applied {
					errorsSeen <- fmt.Errorf("event %s = (%v, %w)", id, applied, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Error(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := fusedb.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	count, found, err := reopened.GetInt64([]byte("tickets/show-1/sold"))
	if err != nil || !found || count != workers*perWorker {
		t.Fatalf("reopened count = (%d, %v, %v), want %d", count, found, err, workers*perWorker)
	}
}

func TestDurablePilotOptionsFailSaferDefaults(t *testing.T) {
	options := fusedb.DurablePilotOptions("/data/fusedb")
	if options.Dir != "/data/fusedb" || !options.WALSyncWrites {
		t.Fatalf("durable options = %+v", options)
	}
	if options.Scheduler.MaxCPUUtilization != 0.70 ||
		options.Scheduler.MaxDiskUtilization != 0.70 ||
		options.Scheduler.MaxMemoryUtilization != 0.70 {
		t.Fatalf("resource headroom = %+v", options.Scheduler)
	}
	if !options.DictionaryTraining.Disabled || options.Metrics.LatencySampleEvery == 0 {
		t.Fatalf("pilot safety toggles = training=%+v metrics=%+v", options.DictionaryTraining, options.Metrics)
	}
}

func BenchmarkApplyOncePurchaseSyncWAL(b *testing.B) {
	options := fusedb.DurablePilotOptions(filepath.Join(b.TempDir(), "db"))
	db, err := fusedb.Open(options)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var sequence atomic.Uint64
	var firstErr atomic.Pointer[error]
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			number := sequence.Add(1)
			id := fmt.Sprintf("%020d", number)
			counterKey := []byte(fmt.Sprintf("tickets/sold/%02d", number%64))
			applied, err := db.ApplyOnce(
				[]byte("event/"+id),
				[]fusedb.Mutation{
					fusedb.PutMutation([]byte("purchase/"+id), []byte("paid")),
					fusedb.IncMutation(counterKey, 1),
				},
			)
			if err != nil || !applied {
				failure := fmt.Errorf("event %s = (%v, %w)", id, applied, err)
				firstErr.CompareAndSwap(nil, &failure)
				return
			}
		}
	})
	if failure := firstErr.Load(); failure != nil {
		b.Fatal(*failure)
	}
	var count int64
	for shard := range 64 {
		value, _, err := db.GetInt64([]byte(fmt.Sprintf("tickets/sold/%02d", shard)))
		if err != nil {
			b.Fatal(err)
		}
		count += value
	}
	if count != int64(b.N) {
		b.Fatalf("count = %d, want %d", count, b.N)
	}
}

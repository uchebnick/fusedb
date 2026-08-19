// Package likestickets demonstrates the durable pilot contract for likes and
// purchased-ticket counters. Event IDs must come from an authoritative outbox
// and remain stable across delivery retries.
package likestickets

import (
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/uchebnick/fusedb/pkg/fusedb"
)

type Store struct {
	db *fusedb.DB
}

type Capacity struct {
	CPUCores           float64
	DiskBytesPerSecond float64
	MemoryBytes        uint64
}

const counterShards = 64

func Open(dir string, capacity Capacity) (*Store, error) {
	options := fusedb.DurablePilotOptions(dir)
	options.Scheduler.CPUCores = capacity.CPUCores
	options.Scheduler.DiskBytesPerSecond = capacity.DiskBytesPerSecond
	options.Scheduler.MemoryBytes = capacity.MemoryBytes
	db, err := fusedb.Open(options)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Like stores one user's membership and increments the materialized count only
// when the membership was absent. A stable eventID makes delivery retries safe.
func (s *Store) Like(eventID, objectID, userID string, at time.Time) (bool, error) {
	if err := validateIDs(eventID, objectID, userID); err != nil {
		return false, err
	}
	likeKey := key("like", objectID, userID)
	countKey := shardedKey("like-count", objectID, userID)
	return s.db.ApplyOnceIf(
		key("event", eventID),
		[]fusedb.Condition{fusedb.KeyAbsent(likeKey)},
		[]fusedb.Mutation{
			fusedb.PutMutation(likeKey, []byte(at.UTC().Format(time.RFC3339Nano))),
			fusedb.IncMutation(countKey, 1),
		},
	)
}

// Unlike removes a membership and decrements the count only when it exists.
// A rejected first evaluation is also remembered, so an old retry cannot
// remove a newer like.
func (s *Store) Unlike(eventID, objectID, userID string) (bool, error) {
	if err := validateIDs(eventID, objectID, userID); err != nil {
		return false, err
	}
	likeKey := key("like", objectID, userID)
	countKey := shardedKey("like-count", objectID, userID)
	return s.db.ApplyOnceIf(
		key("event", eventID),
		[]fusedb.Condition{fusedb.KeyPresent(likeKey)},
		[]fusedb.Mutation{
			fusedb.DeleteMutation(likeKey),
			fusedb.IncMutation(countKey, -1),
		},
	)
}

// RecordPurchase persists the order projection and ticket delta exactly once
// for one outbox event. It does not authorize payment or enforce inventory.
func (s *Store) RecordPurchase(eventID, eventObjectID, orderID string, tickets int64) (bool, error) {
	if err := validateIDs(eventID, eventObjectID, orderID); err != nil {
		return false, err
	}
	if tickets <= 0 {
		return false, errors.New("tickets must be positive")
	}
	return s.db.ApplyOnce(
		key("event", eventID),
		[]fusedb.Mutation{
			fusedb.PutMutation(key("purchase", orderID), []byte("paid")),
			fusedb.IncMutation(shardedKey("tickets-sold", eventObjectID, orderID), tickets),
		},
	)
}

func (s *Store) LikeCount(objectID string) (int64, bool, error) {
	return s.sumCounter("like-count", objectID)
}

func (s *Store) TicketsSold(eventObjectID string) (int64, bool, error) {
	return s.sumCounter("tickets-sold", eventObjectID)
}

func (s *Store) Health() fusedb.HealthStatus { return s.db.Health() }
func (s *Store) Close() error                { return s.db.Close() }

func key(namespace string, parts ...string) []byte {
	result := namespace
	for _, part := range parts {
		result += "/" + base64.RawURLEncoding.EncodeToString([]byte(part))
	}
	return []byte(result)
}

func shardedKey(namespace, objectID, distributionKey string) []byte {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(distributionKey))
	shard := hash.Sum64() % counterShards
	return key(namespace, objectID, strconv.FormatUint(shard, 10))
}

func (s *Store) sumCounter(namespace, objectID string) (int64, bool, error) {
	var total int64
	var any bool
	for shard := uint64(0); shard < counterShards; shard++ {
		value, found, err := s.db.GetInt64(key(namespace, objectID, strconv.FormatUint(shard, 10)))
		if err != nil {
			return 0, false, err
		}
		if found {
			any = true
			total += value
		}
	}
	return total, any, nil
}

func validateIDs(ids ...string) error {
	for index, id := range ids {
		if id == "" {
			return fmt.Errorf("id %d is empty", index)
		}
	}
	return nil
}

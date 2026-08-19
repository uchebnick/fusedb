package oneleafdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/uchebnick/fusedb/internal/limits"
	"github.com/uchebnick/fusedb/internal/ops"
	"github.com/uchebnick/fusedb/internal/value"
	"github.com/uchebnick/fusedb/internal/wal"
)

var (
	ErrEmptyTransaction     = errors.New("oneleafdb: transaction has no mutations")
	ErrTooManyMutations     = errors.New("oneleafdb: transaction has too many mutations")
	ErrDuplicateMutationKey = errors.New("oneleafdb: transaction mutates a key more than once")
	ErrIdempotencyConflict  = errors.New("oneleafdb: idempotency key was already used for different mutations")
)

type conditionKind uint8

const (
	conditionAbsent conditionKind = iota
	conditionPresent
)

// Condition is evaluated while all foreground mutations are excluded.
// Construct values with KeyAbsent and KeyPresent.
type Condition struct {
	kind conditionKind
	key  []byte
}

func KeyAbsent(key []byte) Condition {
	return Condition{kind: conditionAbsent, key: bytes.Clone(key)}
}

func KeyPresent(key []byte) Condition {
	return Condition{kind: conditionPresent, key: bytes.Clone(key)}
}

// Mutation is one member of an atomic transaction. Construct values with
// PutMutation, DeleteMutation, and IncMutation.
type Mutation struct {
	kind  ops.OpKind
	key   []byte
	value []byte
	delta int64
}

func PutMutation(key, raw []byte) Mutation {
	return Mutation{kind: ops.OpPut, key: bytes.Clone(key), value: bytes.Clone(raw)}
}

func DeleteMutation(key []byte) Mutation {
	return Mutation{kind: ops.OpDelete, key: bytes.Clone(key)}
}

func IncMutation(key []byte, delta int64) Mutation {
	return Mutation{kind: ops.OpInc, key: bytes.Clone(key), delta: delta}
}

// Apply evaluates every condition and, when all match, commits all mutations
// in one WAL record and one foreground visibility window. A false result with
// a nil error means at least one condition did not match and nothing changed.
func (db *DB) Apply(conditions []Condition, mutations []Mutation) (bool, error) {
	started := db.telemetry.BeginWrite()
	defer db.telemetry.EndWrite(started)

	if err := db.backgroundErr(); err != nil {
		return false, err
	}
	if err := validateTransaction(conditions, mutations); err != nil {
		return false, err
	}

	unlock := db.lockTransactionKeys(nil, conditions, mutations)
	defer unlock()
	return db.applyLocked(conditions, mutations)
}

// ApplyOnce commits mutations at most once for idempotencyKey. Reusing the key
// with the same ordered mutations returns (false, nil); reusing it for a
// different transaction returns ErrIdempotencyConflict. The marker is stored
// as a normal byte key and must be dedicated to this API.
func (db *DB) ApplyOnce(idempotencyKey []byte, mutations []Mutation) (bool, error) {
	return db.ApplyOnceIf(idempotencyKey, nil, mutations)
}

// ApplyOnceIf evaluates conditions only on the first use of idempotencyKey.
// The decision is persisted even when a condition does not match, preventing
// a delayed retry from applying later against different business state.
func (db *DB) ApplyOnceIf(idempotencyKey []byte, conditions []Condition, mutations []Mutation) (bool, error) {
	started := db.telemetry.BeginWrite()
	defer db.telemetry.EndWrite(started)

	if err := db.backgroundErr(); err != nil {
		return false, err
	}
	if err := validateMutationInput(idempotencyKey, nil); err != nil {
		return false, err
	}
	if err := validateTransaction(conditions, mutations); err != nil {
		return false, err
	}
	for _, mutation := range mutations {
		if bytes.Equal(mutation.key, idempotencyKey) {
			return false, ErrDuplicateMutationKey
		}
	}
	for _, condition := range conditions {
		if bytes.Equal(condition.key, idempotencyKey) {
			return false, ErrDuplicateMutationKey
		}
	}

	wantedMarker, err := transactionMarker(conditions, mutations)
	if err != nil {
		return false, err
	}

	unlock := db.lockTransactionKeys(idempotencyKey, conditions, mutations)
	defer unlock()

	existing, found, err := db.tree.Get(idempotencyKey)
	if err != nil {
		return false, err
	}
	if found {
		if bytes.Equal(existing, wantedMarker) {
			return false, nil
		}
		return false, ErrIdempotencyConflict
	}

	matched, err := db.conditionsMatch(conditions)
	if err != nil {
		return false, err
	}
	withMarker := []Mutation{PutMutation(idempotencyKey, wantedMarker)}
	if matched {
		withMarker = append(withMarker, mutations...)
	}
	committed, err := db.applyLocked(nil, withMarker)
	return matched && committed, err
}

func (db *DB) applyLocked(conditions []Condition, mutations []Mutation) (bool, error) {
	matched, err := db.conditionsMatch(conditions)
	if err != nil || !matched {
		return false, err
	}

	return db.commitMutationsLocked(mutations)
}

func (db *DB) conditionsMatch(conditions []Condition) (bool, error) {
	for _, condition := range conditions {
		_, found, err := db.tree.Get(condition.key)
		if err != nil {
			return false, err
		}
		matched := condition.kind == conditionPresent && found || condition.kind == conditionAbsent && !found
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func (db *DB) commitMutationsLocked(mutations []Mutation) (bool, error) {
	for _, mutation := range mutations {
		if mutation.kind != ops.OpInc {
			continue
		}
		encoded, found, err := db.tree.GetEncoded(mutation.key)
		if err != nil {
			return false, err
		}
		if !found {
			continue
		}
		kind, err := value.KindOf(encoded)
		if err != nil {
			return false, err
		}
		if kind != value.KindInt64 {
			return false, value.ErrKindMismatch
		}
	}

	walMutations := make([]wal.BatchMutation, 0, len(mutations))
	for _, mutation := range mutations {
		walMutation := wal.BatchMutation{Kind: mutation.kind, Key: mutation.key}
		switch mutation.kind {
		case ops.OpPut:
			walMutation.Payload = mutation.value
		case ops.OpInc:
			var encoded [binary.MaxVarintLen64]byte
			size := binary.PutVarint(encoded[:], mutation.delta)
			walMutation.Payload = bytes.Clone(encoded[:size])
		}
		walMutations = append(walMutations, walMutation)
	}

	db.applyMu.RLock()
	payloadBytes := 0
	if db.wal != nil {
		var err error
		_, payloadBytes, err = db.wal.AppendBatch(walMutations)
		if err != nil {
			if errors.Is(err, wal.ErrPersistence) {
				db.recordTerminalError(err)
			}
			db.applyMu.RUnlock()
			return false, err
		}
	}
	db.visibilityMu.Lock()
	for _, mutation := range mutations {
		var err error
		switch mutation.kind {
		case ops.OpPut:
			err = db.tree.Put(mutation.key, mutation.value)
		case ops.OpDelete:
			err = db.tree.Delete(mutation.key)
		case ops.OpInc:
			err = db.tree.Inc(mutation.key, mutation.delta)
		}
		if err != nil {
			db.visibilityMu.Unlock()
			db.applyMu.RUnlock()
			failure := fmt.Errorf("oneleafdb: apply durable transaction: %w", err)
			db.recordBackgroundErr(failure)
			return false, failure
		}
		db.invalidate(mutation.key)
	}
	db.visibilityMu.Unlock()
	db.applyMu.RUnlock()

	if payloadBytes == 0 {
		for _, mutation := range mutations {
			payloadBytes += len(mutation.key) + len(mutation.value) + 8
		}
	}
	db.noteWALGrowth(payloadBytes)
	db.scheduleBackgroundWork()
	return true, nil
}

func (db *DB) lockTransactionKeys(idempotencyKey []byte, conditions []Condition, mutations []Mutation) func() {
	shards := make([]int, 0, len(conditions)+len(mutations)+1)
	if len(idempotencyKey) != 0 {
		shards = append(shards, int(mutationShard(idempotencyKey)))
	}
	for _, condition := range conditions {
		shards = append(shards, int(mutationShard(condition.key)))
	}
	for _, mutation := range mutations {
		shards = append(shards, int(mutationShard(mutation.key)))
	}
	sort.Ints(shards)
	unique := shards[:0]
	for _, shard := range shards {
		if len(unique) == 0 || unique[len(unique)-1] != shard {
			unique = append(unique, shard)
		}
	}
	for _, shard := range unique {
		db.mutationMu[shard].Lock()
	}
	return func() {
		for index := len(unique) - 1; index >= 0; index-- {
			db.mutationMu[unique[index]].Unlock()
		}
	}
}

func validateTransaction(conditions []Condition, mutations []Mutation) error {
	if len(mutations) == 0 {
		return ErrEmptyTransaction
	}
	if len(mutations) > limits.MaxBatchMutations-1 {
		return fmt.Errorf("%w: %d > %d", ErrTooManyMutations, len(mutations), limits.MaxBatchMutations-1)
	}
	for _, condition := range conditions {
		if err := validateMutationInput(condition.key, nil); err != nil {
			return err
		}
		if condition.kind != conditionAbsent && condition.kind != conditionPresent {
			return errors.New("oneleafdb: invalid transaction condition")
		}
	}
	seen := make(map[string]struct{}, len(mutations))
	for _, mutation := range mutations {
		valueBytes := []byte(nil)
		if mutation.kind == ops.OpPut {
			valueBytes = mutation.value
		}
		if err := validateMutationInput(mutation.key, valueBytes); err != nil {
			return err
		}
		switch mutation.kind {
		case ops.OpPut, ops.OpDelete, ops.OpInc:
		default:
			return errors.New("oneleafdb: invalid transaction mutation")
		}
		key := string(mutation.key)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: %x", ErrDuplicateMutationKey, mutation.key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func transactionMarker(conditions []Condition, mutations []Mutation) ([]byte, error) {
	walMutations := make([]wal.BatchMutation, 0, len(mutations))
	for _, mutation := range mutations {
		item := wal.BatchMutation{Kind: mutation.kind, Key: mutation.key}
		switch mutation.kind {
		case ops.OpPut:
			item.Payload = mutation.value
		case ops.OpInc:
			var encoded [binary.MaxVarintLen64]byte
			size := binary.PutVarint(encoded[:], mutation.delta)
			item.Payload = encoded[:size]
		}
		walMutations = append(walMutations, item)
	}
	encoded, err := wal.EncodeBatch(walMutations)
	if err != nil {
		return nil, err
	}
	canonical := make([]byte, 0, len(encoded)+64)
	canonical = append(canonical, 1)
	canonical = binary.AppendUvarint(canonical, uint64(len(conditions)))
	for _, condition := range conditions {
		canonical = append(canonical, byte(condition.kind))
		canonical = binary.AppendUvarint(canonical, uint64(len(condition.key)))
		canonical = append(canonical, condition.key...)
	}
	canonical = append(canonical, encoded...)
	digest := sha256.Sum256(canonical)
	marker := make([]byte, 0, 4+len(digest))
	marker = append(marker, 'F', 'D', 'E', 1)
	marker = append(marker, digest[:]...)
	return marker, nil
}

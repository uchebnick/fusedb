package fusedb

import "github.com/uchebnick/fusedb/internal/engine"

// Condition is a transaction precondition. Construct values with KeyAbsent
// and KeyPresent.
type Condition struct {
	inner engine.Condition
}

// KeyAbsent requires key to be missing when the transaction commits.
func KeyAbsent(key []byte) Condition {
	return Condition{inner: engine.KeyAbsent(key)}
}

// KeyPresent requires key to exist when the transaction commits.
func KeyPresent(key []byte) Condition {
	return Condition{inner: engine.KeyPresent(key)}
}

// Mutation is one member of an atomic transaction. Construct values with
// PutMutation, DeleteMutation, and IncMutation.
type Mutation struct {
	inner engine.Mutation
}

func PutMutation(key, value []byte) Mutation {
	return Mutation{inner: engine.PutMutation(key, value)}
}

func DeleteMutation(key []byte) Mutation {
	return Mutation{inner: engine.DeleteMutation(key)}
}

func IncMutation(key []byte, delta int64) Mutation {
	return Mutation{inner: engine.IncMutation(key, delta)}
}

// Apply atomically evaluates conditions and commits mutations. It returns
// applied=false with a nil error when a condition did not match. Mutations are
// written as one checksummed WAL record and are never partially visible to
// concurrent public API calls.
func (db *DB) Apply(conditions []Condition, mutations []Mutation) (applied bool, err error) {
	convertedConditions := make([]engine.Condition, len(conditions))
	for index, condition := range conditions {
		convertedConditions[index] = condition.inner
	}
	convertedMutations := make([]engine.Mutation, len(mutations))
	for index, mutation := range mutations {
		convertedMutations[index] = mutation.inner
	}
	return db.db.Apply(convertedConditions, convertedMutations)
}

// ApplyOnce commits mutations at most once for idempotencyKey. The key must be
// stable across retries and dedicated to this API. A duplicate call with the
// same ordered mutations returns applied=false; a different mutation set
// returns ErrIdempotencyConflict.
func (db *DB) ApplyOnce(idempotencyKey []byte, mutations []Mutation) (applied bool, err error) {
	return db.ApplyOnceIf(idempotencyKey, nil, mutations)
}

// ApplyOnceIf combines idempotent event processing with transaction
// preconditions. The first evaluation is remembered even when a condition is
// false, so a delayed retry cannot mutate newer state.
func (db *DB) ApplyOnceIf(idempotencyKey []byte, conditions []Condition, mutations []Mutation) (applied bool, err error) {
	convertedConditions := make([]engine.Condition, len(conditions))
	for index, condition := range conditions {
		convertedConditions[index] = condition.inner
	}
	converted := make([]engine.Mutation, len(mutations))
	for index, mutation := range mutations {
		converted[index] = mutation.inner
	}
	return db.db.ApplyOnceIf(idempotencyKey, convertedConditions, converted)
}

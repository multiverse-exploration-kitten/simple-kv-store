package fsm

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// MaxIdempotencyKeys is how many idempotency keys the FSM remembers. The
// table is replicated state: a change of this number is a versioned change.
const MaxIdempotencyKeys = 10000

// ErrIdempotencyKeyReused is the result of a write that reuses an
// idempotency key with a different request. Nothing is written.
var ErrIdempotencyKeyReused = errors.New("idempotency key was already used for a different request")

// Entry is one stored key. Both indexes are Raft log indexes.
type Entry struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	CreateIndex uint64 `json:"create_index"`
	ModifyIndex uint64 `json:"modify_index"`
}

// ApplyResult is what FSM.Apply returns for a put or a delete.
//
//   - Put applied: Applied=true, Entry = the new entry, Existed = the key existed before.
//   - Delete applied: Applied=true, Existed = the key existed. Entry is nil.
//   - CAS failed: Applied=false, Entry = the current entry, or nil if the key does not exist.
//   - Duplicate: the stored result of the first request, with Duplicate=true.
//   - Err holds only business errors (ErrIdempotencyKeyReused). Nothing was written then.
type ApplyResult struct {
	Index     uint64
	Applied   bool
	Existed   bool
	Entry     *Entry
	Duplicate bool
	Err       error
}

// storedResult is the part of an ApplyResult that the dedupe table keeps.
type storedResult struct {
	Index   uint64 `json:"index"`
	Applied bool   `json:"applied"`
	Existed bool   `json:"existed"`
	Entry   *Entry `json:"entry,omitempty"`
}

// idempotencyRecord is immutable after it is created, so snapshots may share it.
type idempotencyRecord struct {
	Key         string       `json:"key"`
	Fingerprint string       `json:"fingerprint"`
	Result      storedResult `json:"result"`
}

// state is the replicated state. It is not safe for concurrent use: FSM
// holds the lock.
type state struct {
	entries          map[string]Entry
	idempotency      map[string]idempotencyRecord
	idempotencyOrder []string // oldest first, so eviction is the same on every node
	lastAppliedIndex uint64
}

func newState() *state {
	return &state{entries: map[string]Entry{}, idempotency: map[string]idempotencyRecord{}}
}

func (current *state) apply(index uint64, command Command) ApplyResult {
	current.lastAppliedIndex = index
	if command.IdempotencyKey == "" {
		return current.execute(index, command)
	}
	fingerprint := requestFingerprint(command)
	if record, found := current.idempotency[command.IdempotencyKey]; found {
		if record.Fingerprint != fingerprint {
			return ApplyResult{Index: index, Err: ErrIdempotencyKeyReused}
		}
		return ApplyResult{
			Index:     record.Result.Index,
			Applied:   record.Result.Applied,
			Existed:   record.Result.Existed,
			Entry:     copyEntry(record.Result.Entry),
			Duplicate: true,
		}
	}
	result := current.execute(index, command)
	current.remember(idempotencyRecord{
		Key:         command.IdempotencyKey,
		Fingerprint: fingerprint,
		Result:      storedResult{Index: result.Index, Applied: result.Applied, Existed: result.Existed, Entry: copyEntry(result.Entry)},
	})
	return result
}

func (current *state) execute(index uint64, command Command) ApplyResult {
	switch command.Type.withoutFlag() {
	case CommandPut:
		return current.put(index, command)
	case CommandDelete:
		return current.delete(index, command)
	}
	panic(fmt.Sprintf("fsm: execute got unknown command type %d", command.Type))
}

func (current *state) put(index uint64, command Command) ApplyResult {
	existing, existed := current.entries[command.Key]
	if !casMatches(command.CASIndex, existing, existed) {
		return ApplyResult{Index: index, Entry: entryIfExists(existing, existed)}
	}
	updated := Entry{Key: command.Key, Value: command.Value, CreateIndex: index, ModifyIndex: index}
	if existed {
		updated.CreateIndex = existing.CreateIndex
	}
	current.entries[command.Key] = updated
	return ApplyResult{Index: index, Applied: true, Existed: existed, Entry: &updated}
}

// delete with CASIndex 0 means "the key must not exist", the same rule as
// for put. It succeeds as a no-op on a missing key.
func (current *state) delete(index uint64, command Command) ApplyResult {
	existing, existed := current.entries[command.Key]
	if !casMatches(command.CASIndex, existing, existed) {
		return ApplyResult{Index: index, Entry: entryIfExists(existing, existed)}
	}
	delete(current.entries, command.Key)
	return ApplyResult{Index: index, Applied: true, Existed: existed}
}

func casMatches(casIndex *uint64, existing Entry, existed bool) bool {
	switch {
	case casIndex == nil:
		return true
	case *casIndex == 0:
		return !existed
	default:
		return existed && existing.ModifyIndex == *casIndex
	}
}

func (current *state) remember(record idempotencyRecord) {
	current.idempotency[record.Key] = record
	current.idempotencyOrder = append(current.idempotencyOrder, record.Key)
	if len(current.idempotencyOrder) > MaxIdempotencyKeys {
		oldest := current.idempotencyOrder[0]
		current.idempotencyOrder = current.idempotencyOrder[1:]
		delete(current.idempotency, oldest)
	}
}

// requestFingerprint identifies the request behind an idempotency key: the
// encoded command without the key.
func requestFingerprint(command Command) string {
	command.IdempotencyKey = ""
	encoded, err := EncodeCommand(command)
	if err != nil {
		panic(fmt.Sprintf("fsm: fingerprint: %v", err))
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func entryIfExists(entry Entry, exists bool) *Entry {
	if !exists {
		return nil
	}
	return &entry
}

func copyEntry(entry *Entry) *Entry {
	if entry == nil {
		return nil
	}
	copied := *entry
	return &copied
}

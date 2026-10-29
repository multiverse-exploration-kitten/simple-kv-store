package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

// snapshotFormatVersion versions the snapshot body. raft's SnapshotMeta.Version
// covers only raft's own metadata.
const snapshotFormatVersion = 1

// FSM implements raft.FSM. Apply and Snapshot run on raft's FSM goroutine.
// Get, List, LastAppliedIndex and Len are safe from any goroutine.
type FSM struct {
	mutex sync.RWMutex
	state *state
}

var _ raft.FSM = (*FSM)(nil)

// New returns an empty FSM.
func New() *FSM {
	return &FSM{state: newState()}
}

// Apply applies one command entry. It returns an ApplyResult, or nil for an
// unknown command type with the ignorable flag. It panics for an entry that
// it cannot apply (unknown type without the flag, or a broken body): a node
// that skipped such an entry would diverge from the others.
func (machine *FSM) Apply(entry *raft.Log) interface{} {
	if len(entry.Data) == 0 {
		panic(fmt.Sprintf("fsm: empty command at index %d", entry.Index))
	}
	commandType := CommandType(entry.Data[0])
	if !commandType.known() {
		if !commandType.ignorable() {
			panic(fmt.Sprintf("fsm: unknown command type %d at index %d: stop so that this node does not diverge", commandType, entry.Index))
		}
		machine.mutex.Lock()
		machine.state.lastAppliedIndex = entry.Index
		machine.mutex.Unlock()
		return nil
	}
	command, err := DecodeCommand(entry.Data)
	if err != nil {
		panic(fmt.Sprintf("fsm: index %d: %v", entry.Index, err))
	}
	command.Type = commandType.withoutFlag()
	machine.mutex.Lock()
	defer machine.mutex.Unlock()
	return machine.state.apply(entry.Index, command)
}

// Get returns the entry of a key from the local state.
func (machine *FSM) Get(key string) (Entry, bool) {
	machine.mutex.RLock()
	defer machine.mutex.RUnlock()
	entry, found := machine.state.entries[key]
	return entry, found
}

// List returns the local entries whose key starts with prefix, sorted by key.
func (machine *FSM) List(prefix string) []Entry {
	machine.mutex.RLock()
	matching := make([]Entry, 0)
	for key, entry := range machine.state.entries {
		if strings.HasPrefix(key, prefix) {
			matching = append(matching, entry)
		}
	}
	machine.mutex.RUnlock()
	sortByKey(matching)
	return matching
}

// LastAppliedIndex is the log index of the last command entry that this FSM
// applied. Use it, not raft.AppliedIndex(), as proof of apply: raft counts
// entries sent to the FSM queue. Barrier, no-op and configuration entries
// do not reach the FSM, so they do not move this index.
func (machine *FSM) LastAppliedIndex() uint64 {
	machine.mutex.RLock()
	defer machine.mutex.RUnlock()
	return machine.state.lastAppliedIndex
}

// Len returns the number of stored keys.
func (machine *FSM) Len() int {
	machine.mutex.RLock()
	defer machine.mutex.RUnlock()
	return len(machine.state.entries)
}

// snapshotBody is the persisted form of the state.
type snapshotBody struct {
	FormatVersion    int                 `json:"format_version"`
	LastAppliedIndex uint64              `json:"last_applied_index"`
	Entries          []Entry             `json:"entries"`          // sorted by key
	IdempotencyKeys  []idempotencyRecord `json:"idempotency_keys"` // oldest first
}

type snapshot struct {
	lastAppliedIndex uint64
	entries          map[string]Entry
	idempotencyKeys  []idempotencyRecord
}

// Snapshot copies the state under the lock. No Apply runs while it runs, so
// it does only the copy. Persist does the slow work.
func (machine *FSM) Snapshot() (raft.FSMSnapshot, error) {
	machine.mutex.RLock()
	defer machine.mutex.RUnlock()
	records := make([]idempotencyRecord, 0, len(machine.state.idempotencyOrder))
	for _, key := range machine.state.idempotencyOrder {
		records = append(records, machine.state.idempotency[key])
	}
	return &snapshot{
		lastAppliedIndex: machine.state.lastAppliedIndex,
		entries:          maps.Clone(machine.state.entries),
		idempotencyKeys:  records,
	}, nil
}

// Persist writes the copy as JSON. It runs while Apply continues.
func (copied *snapshot) Persist(sink raft.SnapshotSink) error {
	body := snapshotBody{
		FormatVersion:    snapshotFormatVersion,
		LastAppliedIndex: copied.lastAppliedIndex,
		Entries:          slices.Collect(maps.Values(copied.entries)),
		IdempotencyKeys:  copied.idempotencyKeys,
	}
	sortByKey(body.Entries)
	if err := json.NewEncoder(sink).Encode(body); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("persist snapshot: %w", err)
	}
	// Return the Close error: it reports a failed fsync of the snapshot file.
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (copied *snapshot) Release() {}

// Restore replaces all state with the snapshot.
func (machine *FSM) Restore(source io.ReadCloser) error {
	defer func() { _ = source.Close() }()
	var body snapshotBody
	if err := json.NewDecoder(source).Decode(&body); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	if body.FormatVersion != snapshotFormatVersion {
		return fmt.Errorf("restore snapshot: unknown format version %d", body.FormatVersion)
	}
	restored := newState()
	restored.lastAppliedIndex = body.LastAppliedIndex
	for _, entry := range body.Entries {
		restored.entries[entry.Key] = entry
	}
	for _, record := range body.IdempotencyKeys {
		restored.idempotency[record.Key] = record
		restored.idempotencyOrder = append(restored.idempotencyOrder, record.Key)
	}
	machine.mutex.Lock()
	machine.state = restored
	machine.mutex.Unlock()
	return nil
}

func sortByKey(entries []Entry) {
	slices.SortFunc(entries, func(left, right Entry) int { return strings.Compare(left.Key, right.Key) })
}

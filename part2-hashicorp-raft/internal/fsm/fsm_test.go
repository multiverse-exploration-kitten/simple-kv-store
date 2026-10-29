package fsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
)

func casIndex(index uint64) *uint64 { return &index }

func encodeOrFail(t *testing.T, command Command) []byte {
	t.Helper()
	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatalf("encode %+v: %v", command, err)
	}
	return data
}

func applyCommand(t *testing.T, machine *FSM, index uint64, command Command) ApplyResult {
	t.Helper()
	response := machine.Apply(&raft.Log{Index: index, Type: raft.LogCommand, Data: encodeOrFail(t, command)})
	result, isResult := response.(ApplyResult)
	if !isResult {
		t.Fatalf("Apply returned %T (%v), want ApplyResult", response, response)
	}
	return result
}

// memorySink is a raft.SnapshotSink that keeps the snapshot in memory.
type memorySink struct {
	bytes.Buffer
	closed    bool
	cancelled bool
}

func (sink *memorySink) ID() string    { return "memory" }
func (sink *memorySink) Close() error  { sink.closed = true; return nil }
func (sink *memorySink) Cancel() error { sink.cancelled = true; return nil }

func snapshotBytes(t *testing.T, machine *FSM) []byte {
	t.Helper()
	snapshot, err := machine.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	defer snapshot.Release()
	sink := &memorySink{}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if !sink.closed || sink.cancelled {
		t.Fatalf("Persist must close the sink on success: closed=%v cancelled=%v", sink.closed, sink.cancelled)
	}
	return sink.Bytes()
}

func restoreFrom(t *testing.T, snapshot []byte) *FSM {
	t.Helper()
	machine := New()
	if err := machine.Restore(io.NopCloser(bytes.NewReader(snapshot))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return machine
}

func mustGet(t *testing.T, machine *FSM, key string) Entry {
	t.Helper()
	entry, found := machine.Get(key)
	if !found {
		t.Fatalf("key %q not found", key)
	}
	return entry
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := Command{Type: CommandPut, Key: "k", Value: "v", CASIndex: casIndex(0), IdempotencyKey: "request-1"}
	data := encodeOrFail(t, original)
	if data[0] != byte(CommandPut) {
		t.Fatalf("first byte = %d, want the type byte %d", data[0], CommandPut)
	}
	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf("decoded %+v, want %+v", decoded, original)
	}
	if _, err := DecodeCommand(nil); err == nil {
		t.Fatal("DecodeCommand(nil) must fail")
	}
}

// The idempotency fingerprint is a hash of the encoded bytes, and snapshots
// store it. A node that encodes a command in another way (for example with
// SetEscapeHTML(false), to make entries smaller) disagrees with the
// fingerprints of older nodes during a rolling upgrade (Raft rule 2).
func TestEncodeCommandBytesStayTheSame(t *testing.T) {
	command := Command{Type: CommandPut, Key: "k", Value: "<a & b>\x00", CASIndex: casIndex(7), IdempotencyKey: "id"}
	want := "\x01" + `{"key":"k","value":"\u003ca \u0026 b\u003e\u0000","cas_index":7,"idempotency_key":"id"}`
	if got := string(encodeOrFail(t, command)); got != want {
		t.Fatalf("EncodeCommand = %q, want %q", got, want)
	}
}

func TestPutCreatesThenUpdatesEntry(t *testing.T) {
	machine := New()
	created := applyCommand(t, machine, 5, Command{Type: CommandPut, Key: "a", Value: "1"})
	if !created.Applied || created.Index != 5 || *created.Entry != (Entry{Key: "a", Value: "1", CreateIndex: 5, ModifyIndex: 5}) {
		t.Fatalf("create result = %+v (entry %+v)", created, created.Entry)
	}
	updated := applyCommand(t, machine, 7, Command{Type: CommandPut, Key: "a", Value: "2"})
	want := Entry{Key: "a", Value: "2", CreateIndex: 5, ModifyIndex: 7}
	if !updated.Applied || !updated.Existed || *updated.Entry != want {
		t.Fatalf("update result = %+v (entry %+v), want entry %+v", updated, updated.Entry, want)
	}
	if got := mustGet(t, machine, "a"); got != want {
		t.Fatalf("Get = %+v, want %+v", got, want)
	}
	if machine.LastAppliedIndex() != 7 || machine.Len() != 1 {
		t.Fatalf("LastAppliedIndex=%d Len=%d, want 7 and 1", machine.LastAppliedIndex(), machine.Len())
	}
}

func TestDeleteReportsWhetherTheKeyExisted(t *testing.T) {
	machine := New()
	applyCommand(t, machine, 3, Command{Type: CommandPut, Key: "a", Value: "1"})
	deleted := applyCommand(t, machine, 4, Command{Type: CommandDelete, Key: "a"})
	if !deleted.Applied || !deleted.Existed || deleted.Index != 4 {
		t.Fatalf("delete result = %+v", deleted)
	}
	if _, found := machine.Get("a"); found {
		t.Fatal("key still present after delete")
	}
	missing := applyCommand(t, machine, 5, Command{Type: CommandDelete, Key: "a"})
	if !missing.Applied || missing.Existed {
		t.Fatalf("delete of a missing key = %+v, want Applied and not Existed", missing)
	}
}

func TestCompareAndSet(t *testing.T) {
	existing := Entry{Key: "a", Value: "old", CreateIndex: 10, ModifyIndex: 10}
	testCases := []struct {
		name        string
		command     Command
		wantApplied bool
		wantExisted bool
		wantEntry   *Entry
		wantStored  *Entry
	}{
		{
			name:        "create only on a missing key",
			command:     Command{Type: CommandPut, Key: "b", Value: "new", CASIndex: casIndex(0)},
			wantApplied: true,
			wantEntry:   &Entry{Key: "b", Value: "new", CreateIndex: 20, ModifyIndex: 20},
		},
		{
			name:       "create only on an existing key fails",
			command:    Command{Type: CommandPut, Key: "a", Value: "new", CASIndex: casIndex(0)},
			wantEntry:  &existing,
			wantStored: &existing,
		},
		{
			name:        "matching index writes",
			command:     Command{Type: CommandPut, Key: "a", Value: "new", CASIndex: casIndex(10)},
			wantApplied: true,
			wantExisted: true,
			wantEntry:   &Entry{Key: "a", Value: "new", CreateIndex: 10, ModifyIndex: 20},
			wantStored:  &Entry{Key: "a", Value: "new", CreateIndex: 10, ModifyIndex: 20},
		},
		{
			name:       "other index fails",
			command:    Command{Type: CommandPut, Key: "a", Value: "new", CASIndex: casIndex(9)},
			wantEntry:  &existing,
			wantStored: &existing,
		},
		{
			name:    "non-zero index on a missing key fails",
			command: Command{Type: CommandPut, Key: "b", Value: "new", CASIndex: casIndex(10)},
		},
		{
			name:        "delete with matching index",
			command:     Command{Type: CommandDelete, Key: "a", CASIndex: casIndex(10)},
			wantApplied: true,
			wantExisted: true,
		},
		{
			name:       "delete with other index fails",
			command:    Command{Type: CommandDelete, Key: "a", CASIndex: casIndex(11)},
			wantEntry:  &existing,
			wantStored: &existing,
		},
		{
			name:    "delete with non-zero index on a missing key fails",
			command: Command{Type: CommandDelete, Key: "b", CASIndex: casIndex(10)},
		},
		{
			name:       "delete with index 0 on an existing key fails",
			command:    Command{Type: CommandDelete, Key: "a", CASIndex: casIndex(0)},
			wantEntry:  &existing,
			wantStored: &existing,
		},
		{
			name:        "delete with index 0 on a missing key succeeds",
			command:     Command{Type: CommandDelete, Key: "b", CASIndex: casIndex(0)},
			wantApplied: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			machine := New()
			applyCommand(t, machine, 10, Command{Type: CommandPut, Key: "a", Value: "old"})
			result := applyCommand(t, machine, 20, testCase.command)
			if result.Applied != testCase.wantApplied || result.Existed != testCase.wantExisted || result.Index != 20 {
				t.Fatalf("result = %+v, want Applied=%v Existed=%v Index=20", result, testCase.wantApplied, testCase.wantExisted)
			}
			if !reflect.DeepEqual(result.Entry, testCase.wantEntry) {
				t.Fatalf("result entry = %+v, want %+v", result.Entry, testCase.wantEntry)
			}
			stored, found := machine.Get("a")
			switch {
			case testCase.wantStored != nil && (!found || stored != *testCase.wantStored):
				t.Fatalf("stored a = %+v (found %v), want %+v", stored, found, *testCase.wantStored)
			case testCase.wantStored == nil && testCase.command.Key == "a" && found:
				t.Fatalf("a still stored: %+v", stored)
			}
		})
	}
}

func TestDuplicateReturnsStoredResultWithoutWriting(t *testing.T) {
	machine := New()
	command := Command{Type: CommandPut, Key: "a", Value: "1", CASIndex: casIndex(0), IdempotencyKey: "retry-me"}
	first := applyCommand(t, machine, 10, command)
	if !first.Applied || first.Duplicate {
		t.Fatalf("first result = %+v", first)
	}
	// Without the key, this retry of a create-only write would fail the CAS.
	retry := applyCommand(t, machine, 11, command)
	if !retry.Duplicate || !retry.Applied || retry.Index != 10 || *retry.Entry != *first.Entry {
		t.Fatalf("retry result = %+v (entry %+v), want the first result with Duplicate=true", retry, retry.Entry)
	}
	if got := mustGet(t, machine, "a"); got.ModifyIndex != 10 {
		t.Fatalf("duplicate changed the entry: %+v", got)
	}
	if machine.LastAppliedIndex() != 11 {
		t.Fatalf("LastAppliedIndex = %d, want 11", machine.LastAppliedIndex())
	}
}

func TestIdempotencyKeyReusedForOtherRequestIsRejected(t *testing.T) {
	machine := New()
	applyCommand(t, machine, 10, Command{Type: CommandPut, Key: "a", Value: "1", IdempotencyKey: "shared"})
	reused := applyCommand(t, machine, 11, Command{Type: CommandPut, Key: "a", Value: "2", IdempotencyKey: "shared"})
	if !errors.Is(reused.Err, ErrIdempotencyKeyReused) || reused.Applied || reused.Duplicate {
		t.Fatalf("reused result = %+v, want ErrIdempotencyKeyReused", reused)
	}
	if got := mustGet(t, machine, "a"); got.Value != "1" {
		t.Fatalf("value = %q, want 1", got.Value)
	}
	otherType := applyCommand(t, machine, 12, Command{Type: CommandDelete, Key: "a", IdempotencyKey: "shared"})
	if !errors.Is(otherType.Err, ErrIdempotencyKeyReused) {
		t.Fatalf("delete with a put's key = %+v, want ErrIdempotencyKeyReused", otherType)
	}
}

func TestIdempotencyTableEvictsOldestKeyAtLimit(t *testing.T) {
	machine := New()
	commandFor := func(number int) Command {
		return Command{Type: CommandPut, Key: "counter", Value: fmt.Sprint(number), IdempotencyKey: fmt.Sprintf("request-%d", number)}
	}
	for number := 0; number <= MaxIdempotencyKeys; number++ {
		applyCommand(t, machine, uint64(number+1), commandFor(number))
	}
	nextIndex := uint64(MaxIdempotencyKeys + 2)
	stillKnown := applyCommand(t, machine, nextIndex, commandFor(1))
	if !stillKnown.Duplicate {
		t.Fatalf("request-1 should still be in the table: %+v", stillKnown)
	}
	evicted := applyCommand(t, machine, nextIndex+1, commandFor(0))
	if evicted.Duplicate || !evicted.Applied || evicted.Index != nextIndex+1 {
		t.Fatalf("request-0 should be evicted and run again: %+v", evicted)
	}
}

func TestSnapshotRoundTripKeepsDataAndIdempotencyTable(t *testing.T) {
	original := New()
	applyCommand(t, original, 1, Command{Type: CommandPut, Key: "a", Value: "1"})
	applyCommand(t, original, 2, Command{Type: CommandPut, Key: "b", Value: "2", IdempotencyKey: "put-b"})
	applyCommand(t, original, 3, Command{Type: CommandDelete, Key: "a"})
	applyCommand(t, original, 4, Command{Type: CommandPut, Key: "c", Value: "3"})

	restored := restoreFrom(t, snapshotBytes(t, original))
	if !reflect.DeepEqual(restored.List(""), original.List("")) {
		t.Fatalf("entries after restore = %+v, want %+v", restored.List(""), original.List(""))
	}
	if restored.LastAppliedIndex() != 4 || restored.Len() != 2 {
		t.Fatalf("LastAppliedIndex=%d Len=%d, want 4 and 2", restored.LastAppliedIndex(), restored.Len())
	}
	retry := applyCommand(t, restored, 5, Command{Type: CommandPut, Key: "b", Value: "2", IdempotencyKey: "put-b"})
	if !retry.Duplicate || retry.Index != 2 {
		t.Fatalf("idempotency table lost in snapshot: %+v", retry)
	}
}

func TestRestoreReplacesAllState(t *testing.T) {
	source := New()
	applyCommand(t, source, 1, Command{Type: CommandPut, Key: "kept", Value: "1"})
	target := New()
	applyCommand(t, target, 1, Command{Type: CommandPut, Key: "old", Value: "x", IdempotencyKey: "old-request"})
	if err := target.Restore(io.NopCloser(bytes.NewReader(snapshotBytes(t, source)))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, found := target.Get("old"); found {
		t.Fatal("old key survived the restore")
	}
	again := applyCommand(t, target, 2, Command{Type: CommandPut, Key: "old", Value: "x", IdempotencyKey: "old-request"})
	if again.Duplicate {
		t.Fatal("old idempotency key survived the restore")
	}
}

func TestRestoreRejectsUnknownSnapshotFormat(t *testing.T) {
	err := New().Restore(io.NopCloser(strings.NewReader(`{"format_version": 99}`)))
	if err == nil {
		t.Fatal("Restore accepted an unknown snapshot format")
	}
}

func TestListReturnsMatchingEntriesSortedByKey(t *testing.T) {
	machine := New()
	for index, key := range []string{"user/b", "other", "user/a", "user/c"} {
		applyCommand(t, machine, uint64(index+1), Command{Type: CommandPut, Key: key, Value: "v"})
	}
	var keys []string
	for _, entry := range machine.List("user/") {
		keys = append(keys, entry.Key)
	}
	if want := []string{"user/a", "user/b", "user/c"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("List keys = %v, want %v", keys, want)
	}
}

// randomLog builds a reproducible log with puts, deletes, CAS writes and
// retried idempotency keys.
func randomLog(seed uint64, length int) []Command {
	random := rand.New(rand.NewPCG(seed, seed))
	commands := make([]Command, 0, length)
	for range length {
		command := Command{Key: fmt.Sprintf("key-%d", random.IntN(8))}
		if random.IntN(4) == 0 {
			command.Type = CommandDelete
		} else {
			command.Type = CommandPut
			command.Value = fmt.Sprint(random.IntN(1000))
		}
		if random.IntN(3) == 0 {
			command.CASIndex = casIndex(uint64(random.IntN(len(commands) + 1)))
		}
		if random.IntN(2) == 0 {
			command.IdempotencyKey = fmt.Sprintf("request-%d", random.IntN(40))
		}
		commands = append(commands, command)
	}
	return commands
}

// Scenario: one FSM applies a log from start to end. A second FSM applies the
// first half, is snapshotted and restored into a third FSM (like a follower
// that installs a snapshot), and the third FSM applies the second half.
// Expected: every apply result and the final state are equal.
// Raft lesson: apply must be deterministic, and the snapshot must hold all
// replicated state (data, dedupe table, its order, last index), or nodes diverge.
func TestNodesThatApplySameLogHaveEqualState(t *testing.T) {
	commands := randomLog(42, 400)
	straight := New()
	firstHalf := New()
	var straightResults, splitResults []ApplyResult
	for index, command := range commands[:200] {
		straightResults = append(straightResults, applyCommand(t, straight, uint64(index+1), command))
		splitResults = append(splitResults, applyCommand(t, firstHalf, uint64(index+1), command))
	}
	secondHalf := restoreFrom(t, snapshotBytes(t, firstHalf))
	for offset, command := range commands[200:] {
		index := uint64(200 + offset + 1)
		straightResults = append(straightResults, applyCommand(t, straight, index, command))
		splitResults = append(splitResults, applyCommand(t, secondHalf, index, command))
	}
	if !reflect.DeepEqual(straightResults, splitResults) {
		t.Fatal("apply results differ between the two FSMs")
	}
	if !bytes.Equal(snapshotBytes(t, straight), snapshotBytes(t, secondHalf)) {
		t.Fatal("final state differs between the two FSMs")
	}
}

func TestUnknownIgnorableCommandIsSkipped(t *testing.T) {
	machine := New()
	applyCommand(t, machine, 1, Command{Type: CommandPut, Key: "a", Value: "1"})
	futureCommand := append([]byte{byte(IgnorableFlag | 77)}, []byte(`{"a format": "from a newer version"`)...)
	response := machine.Apply(&raft.Log{Index: 2, Type: raft.LogCommand, Data: futureCommand})
	if response != nil {
		t.Fatalf("Apply of an ignorable unknown type returned %v, want nil", response)
	}
	if machine.Len() != 1 || machine.LastAppliedIndex() != 2 {
		t.Fatalf("Len=%d LastAppliedIndex=%d, want 1 and 2", machine.Len(), machine.LastAppliedIndex())
	}
}

func TestKnownTypeWithIgnorableFlagIsApplied(t *testing.T) {
	machine := New()
	applyCommand(t, machine, 1, Command{Type: CommandPut | IgnorableFlag, Key: "a", Value: "1"})
	if got := mustGet(t, machine, "a"); got.Value != "1" {
		t.Fatalf("value = %q, want 1", got.Value)
	}
}

func TestCommandThatCannotBeAppliedPanics(t *testing.T) {
	testCases := map[string][]byte{
		"unknown type without the ignorable flag": append([]byte{77}, []byte(`{"key":"a"}`)...),
		"known type with a broken body":           append([]byte{byte(CommandPut)}, []byte(`{"key":`)...),
		"empty entry":                             {},
	}
	for name, data := range testCases {
		t.Run(name, func(t *testing.T) {
			machine := New()
			defer func() {
				if recovered := recover(); recovered == nil {
					t.Fatal("Apply did not panic")
				}
				if machine.LastAppliedIndex() != 0 {
					t.Fatalf("LastAppliedIndex = %d, want 0", machine.LastAppliedIndex())
				}
			}()
			machine.Apply(&raft.Log{Index: 1, Type: raft.LogCommand, Data: data})
		})
	}
}

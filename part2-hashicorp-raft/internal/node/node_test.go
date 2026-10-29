package node

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
)

func startSingleNodeLeader(t *testing.T) *Node {
	t.Helper()
	cfg := recoveryTestConfig(t.TempDir(), []Server{{ID: "node1", Address: "address"}})
	return recoveryTestStart(t, cfg, recoveryTestInmemTransport("address"))
}

// MaxValueBytes counts decoded bytes, but the Raft entry holds JSON, and
// JSON writes '<', '>', '&' and control characters as \u00XX: 6 bytes for
// one. Without a limit on the encoded command, a value at the limit makes a
// 3 MiB Raft entry.
func TestPutRejectsValueWhoseEncodedCommandIsTooLarge(t *testing.T) {
	leader := startSingleNodeLeader(t)
	escapedValue := strings.Repeat("<", MaxValueBytes)

	_, err := leader.Put(context.Background(), "escaped", escapedValue, WriteOptions{})
	var invalid *ValidationError
	if !errors.As(err, &invalid) || !invalid.TooLarge || invalid.Field != "value" {
		t.Fatalf("put of %d bytes that JSON escapes = %v, want a ValidationError on value with TooLarge (HTTP 413)", len(escapedValue), err)
	}
	encoded, err := fsm.EncodeCommand(fsm.Command{Type: fsm.CommandPut, Key: "escaped", Value: escapedValue})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, size := range []int{len(encoded), MaxCommandBytes} {
		if !strings.Contains(invalid.Message, strconv.Itoa(size)) {
			t.Fatalf("message %q does not give the size %d (encoded %d bytes, limit %d)", invalid.Message, size, len(encoded), MaxCommandBytes)
		}
	}
	if _, found := leader.fsm.Get("escaped"); found {
		t.Fatal("the rejected value was written")
	}
}

// The command limit must not cut the documented input limits: a plain value
// of MaxValueBytes fits with the longest key, idempotency key and CAS index,
// also when the key and the idempotency key are all escaped characters.
func TestPutAcceptsLargestPlainValueWithLongestEscapedKeys(t *testing.T) {
	leader := startSingleNodeLeader(t)
	key := strings.Repeat("<", MaxKeyBytes)
	idempotencyKey := strings.Repeat("&", MaxIdempotencyKeyBytes)
	largestCASIndex := uint64(math.MaxUint64)

	result, err := leader.Put(context.Background(), key, strings.Repeat("v", MaxValueBytes), WriteOptions{CASIndex: &largestCASIndex, IdempotencyKey: idempotencyKey})
	if err != nil {
		t.Fatalf("put of the largest plain input: %v", err)
	}
	// The key does not exist, so the CAS check fails. The command still went
	// through raft.
	if result.Applied || result.Index == 0 {
		t.Fatalf("result = %+v, want a failed CAS with a Raft index", result)
	}
}

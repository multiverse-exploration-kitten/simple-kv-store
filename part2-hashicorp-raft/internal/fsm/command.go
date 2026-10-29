// Package fsm is the replicated key-value state machine. Every node applies
// the same Raft log to it and must reach the same state.
package fsm

import (
	"encoding/json"
	"errors"
	"fmt"
)

// CommandType is the first byte of every log entry. The numbers are stored in
// logs and snapshots: only add new values, never reuse or change one.
type CommandType byte

const (
	CommandPut    CommandType = 1
	CommandDelete CommandType = 2

	// IgnorableFlag marks a type that a node may skip when it does not know
	// the type (Consul IgnoreUnknownTypeFlag). Set it only on commands whose
	// effect an older node may safely miss during a rolling upgrade.
	IgnorableFlag CommandType = 128
)

func (commandType CommandType) withoutFlag() CommandType { return commandType &^ IgnorableFlag }

func (commandType CommandType) ignorable() bool { return commandType&IgnorableFlag != 0 }

func (commandType CommandType) known() bool {
	switch commandType.withoutFlag() {
	case CommandPut, CommandDelete:
		return true
	}
	return false
}

// Command is one write. The leader builds it from validated input, so apply
// needs no clock, randomness or I/O.
type Command struct {
	Type  CommandType `json:"-"`
	Key   string      `json:"key"`
	Value string      `json:"value,omitempty"`
	// CASIndex nil = no check. 0 = the key must not exist. N = the key's
	// ModifyIndex must be N.
	CASIndex       *uint64 `json:"cas_index,omitempty"`
	IdempotencyKey string  `json:"idempotency_key,omitempty"`
}

// EncodeCommand writes the type byte and then the JSON body. JSON escapes
// '<', '>', '&' and control characters to 6 bytes each, so an entry can be
// 6 times its input. Callers limit the encoded size. Keep the bytes for an
// input the same: the idempotency fingerprint hashes them, and nodes of two
// versions must agree on it.
func EncodeCommand(command Command) ([]byte, error) {
	body, err := json.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("encode command: %w", err)
	}
	return append([]byte{byte(command.Type)}, body...), nil
}

// DecodeCommand reads a command written by EncodeCommand. Type keeps the
// ignorable flag if it was set.
func DecodeCommand(data []byte) (Command, error) {
	if len(data) == 0 {
		return Command{}, errors.New("decode command: empty entry")
	}
	var command Command
	if err := json.Unmarshal(data[1:], &command); err != nil {
		return Command{}, fmt.Errorf("decode command of type %d: %w", data[0], err)
	}
	command.Type = CommandType(data[0])
	return command, nil
}

package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	bolterrors "go.etcd.io/bbolt/errors"
)

const recoveryTestWaitTimeout = 10 * time.Second

func recoveryTestConfig(dataDir string, initialServers []Server) Config {
	return Config{
		NodeID:         "node1",
		DataDir:        dataDir,
		Bootstrap:      true,
		InitialServers: initialServers,
		Timing:         TestTiming(),
		CommandTimeout: 2 * time.Second,
		Logger:         hclog.NewNullLogger(),
	}
}

// recoveryTestStart starts a node on an in-memory transport and waits until
// it is a ready leader. t.Cleanup closes it.
func recoveryTestStart(t *testing.T, cfg Config, deps Dependencies) *Node {
	t.Helper()
	started, err := New(cfg, deps)
	if err != nil {
		t.Fatalf("start node: %v", err)
	}
	t.Cleanup(func() { _ = started.Close() })
	deadline := time.Now().Add(recoveryTestWaitTimeout)
	for !started.IsLeader() || started.Ready() != nil {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s to be a ready leader (status %+v)", recoveryTestWaitTimeout, cfg.NodeID, started.Status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	return started
}

func recoveryTestPut(t *testing.T, writer *Node, key, value string) WriteResult {
	t.Helper()
	result, err := writer.Put(context.Background(), key, value, WriteOptions{})
	if err != nil || !result.Applied {
		t.Fatalf("put %s=%s: %+v, %v", key, value, result, err)
	}
	return result
}

func recoveryTestInmemTransport(address string) Dependencies {
	_, transport := raft.NewInmemTransport(raft.ServerAddress(address))
	return Dependencies{Transport: transport}
}

// RecoverCluster must open the same raft.db and snapshot directory as New:
// it moves the log into a new snapshot and deletes the log, so a wrong path
// loses all data. The new configuration (here: a new address, as after an
// IP change) must replace the old one.
func TestRecoverClusterKeepsDataAndInstallsNewConfiguration(t *testing.T) {
	dataDir := t.TempDir()
	cfg := recoveryTestConfig(dataDir, []Server{{ID: "node1", Address: "old-address"}})
	before := recoveryTestStart(t, cfg, recoveryTestInmemTransport("old-address"))
	inSnapshot := recoveryTestPut(t, before, "in-snapshot", "1")
	if err := before.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	inLog := recoveryTestPut(t, before, "in-log", "2")
	if err := before.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	deps := recoveryTestInmemTransport("new-address")
	if err := RecoverCluster(cfg, deps, []Server{{ID: "node1", Address: "new-address"}}); err != nil {
		t.Fatalf("RecoverCluster: %v", err)
	}
	after := recoveryTestStart(t, cfg, deps)

	servers, err := after.Configuration()
	if err != nil {
		t.Fatalf("configuration: %v", err)
	}
	if want := []ServerInfo{{ID: "node1", Address: "new-address", Suffrage: SuffrageVoter}}; len(servers) != 1 || servers[0] != want[0] {
		t.Fatalf("configuration after recovery = %+v, want %+v", servers, want)
	}
	for _, written := range []WriteResult{inSnapshot, inLog} {
		read, err := after.Get(context.Background(), written.Entry.Key, ReadOptions{})
		if err != nil || !read.Found || read.Entry != *written.Entry {
			t.Fatalf("read %s after recovery = %+v, %v; want %+v", written.Entry.Key, read, err, *written.Entry)
		}
	}
	// Clients use ModifyIndex for CAS, so new indexes must stay above the old ones.
	if afterRecovery := recoveryTestPut(t, after, "after-recovery", "3"); afterRecovery.Index <= inLog.Index {
		t.Fatalf("index of the first write after recovery = %d, want > %d", afterRecovery.Index, inLog.Index)
	}
}

// An empty data dir is an operator error (a wrong path). raft refuses it, so
// RecoverCluster must not bootstrap a fresh cluster instead.
func TestRecoverClusterRefusesEmptyDataDir(t *testing.T) {
	cfg := recoveryTestConfig(t.TempDir(), []Server{{ID: "node1", Address: "address"}})
	err := RecoverCluster(cfg, recoveryTestInmemTransport("address"), []Server{{ID: "node1", Address: "address"}})
	if err == nil || !strings.Contains(err.Error(), "no initial state") {
		t.Fatalf("RecoverCluster on an empty data dir = %v, want the 'no initial state' refusal", err)
	}
}

// A running node holds the bbolt file lock. RecoverCluster must give up after
// the open timeout and leave the node unharmed, not wait forever.
func TestRecoverClusterFailsWhileTheNodeRuns(t *testing.T) {
	cfg := recoveryTestConfig(t.TempDir(), []Server{{ID: "node1", Address: "address"}})
	running := recoveryTestStart(t, cfg, recoveryTestInmemTransport("address"))

	err := RecoverCluster(cfg, recoveryTestInmemTransport("other-address"), []Server{{ID: "node1", Address: "other-address"}})
	if !errors.Is(err, bolterrors.ErrTimeout) {
		t.Fatalf("RecoverCluster on a running node = %v, want the bbolt timeout error", err)
	}
	recoveryTestPut(t, running, "after-failed-recovery", "ok")
}

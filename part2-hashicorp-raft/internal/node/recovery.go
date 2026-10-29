package node

import (
	"errors"
	"fmt"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/fsm"
)

// RecoverCluster replaces the Raft configuration in cfg.DataDir with servers,
// all as voters. Use it when the cluster lost its quorum for good. It wraps
// raft.RecoverCluster and opens the same stores as New.
//
// Stop the node first: a running node holds the raft.db lock, and
// RecoverCluster fails after the open timeout. Run it on every server that
// stays, with the same servers, and then start each one with New.
// RecoverCluster does not close deps.Transport. raft uses the transport only
// to encode the peers of the new snapshot.
//
// WARNING: after RecoverCluster, every entry of the local log is committed.
// That includes writes that the old cluster never committed. Committed writes
// that only the lost servers stored are gone (reference Q10).
func RecoverCluster(cfg Config, deps Dependencies, servers []Server) (err error) {
	if deps.Transport == nil {
		return errors.New("node: Dependencies.Transport is required")
	}
	cfg = withDefaults(cfg, deps.Transport)
	if err := validateConfig(cfg); err != nil {
		return err
	}

	// The same stores as New. The open timeout makes the call fail instead of
	// waiting forever while a node still runs on this data dir.
	stores, err := openStores(cfg, deps)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := stores.boltStore.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("node: close raft.db: %w", closeErr))
		}
	}()

	configuration := raft.Configuration{}
	for _, server := range servers {
		configuration.Servers = append(configuration.Servers, raft.Server{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(server.ID),
			Address:  raft.ServerAddress(server.Address),
		})
	}
	// raft applies the whole log to this FSM only to write the new snapshot.
	// New restores its own fresh FSM from that snapshot.
	err = raft.RecoverCluster(buildRaftConfig(cfg, nil), fsm.New(), stores.logStore, stores.boltStore, stores.snapshots, deps.Transport, configuration)
	if err != nil {
		return fmt.Errorf("node: recover cluster: %w", err)
	}
	cfg.Logger.Warn("replaced the Raft configuration: every entry of the local log is now committed", "servers", servers)
	return nil
}

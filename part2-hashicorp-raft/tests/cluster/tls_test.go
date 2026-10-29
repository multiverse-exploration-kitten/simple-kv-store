package cluster

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/tlsstream"
)

const (
	tlsIntruderID = "intruder"
	// The text of x509.UnknownAuthorityError: the peer certificate is from
	// another CA.
	tlsUnknownAuthorityText = "certificate signed by unknown authority"
	tlsLogWaitTimeout       = 10 * time.Second
)

// tlsCertificates holds a cluster CA with certificates for node1 to node3,
// and another CA with a certificate for the intruder. Every certificate
// names 127.0.0.1, so for the intruder only the CA is wrong.
type tlsCertificates struct {
	clusterDir string
	otherCADir string
}

func newTLSCertificates(t *testing.T) tlsCertificates {
	t.Helper()
	certificates := tlsCertificates{clusterDir: t.TempDir(), otherCADir: t.TempDir()}
	identity := func(name string) tlsstream.NodeIdentity {
		return tlsstream.NodeIdentity{Name: name, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}}
	}
	var clusterNodes []tlsstream.NodeIdentity
	for number := 1; number <= 3; number++ {
		clusterNodes = append(clusterNodes, identity(fmt.Sprintf("node%d", number)))
	}
	if err := tlsstream.GenerateCertificates(certificates.clusterDir, clusterNodes); err != nil {
		t.Fatalf("generate cluster certificates: %v", err)
	}
	if err := tlsstream.GenerateCertificates(certificates.otherCADir, []tlsstream.NodeIdentity{identity(tlsIntruderID)}); err != nil {
		t.Fatalf("generate the intruder certificate: %v", err)
	}
	return certificates
}

// tlsConfig returns the mutual-TLS config of nodeID. Every config trusts the
// cluster CA. The intruder's own certificate is from the other CA.
func (certificates tlsCertificates) tlsConfig(nodeID string) (*tls.Config, error) {
	certificateDir := certificates.clusterDir
	if nodeID == tlsIntruderID {
		certificateDir = certificates.otherCADir
	}
	certFile, keyFile := tlsstream.NodeCertificateFiles(certificateDir, nodeID)
	return tlsstream.LoadMutualTLS(filepath.Join(certificates.clusterDir, tlsstream.CACertificateFile), certFile, keyFile)
}

func (certificates tlsCertificates) newTransport(nodeID, bindAddress string, logger hclog.Logger) (*raft.NetworkTransport, error) {
	tlsConfig, err := certificates.tlsConfig(nodeID)
	if err != nil {
		return nil, err
	}
	layer, err := tlsstream.NewStreamLayer(bindAddress, nil, tlsConfig)
	if err != nil {
		return nil, err
	}
	return raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  layer,
		MaxPool: transportMaxPool,
		Timeout: transportTimeout,
		Logger:  logger,
	}), nil
}

// clusterOptions makes every harness transport a mutual-TLS transport. The
// transport logs of all nodes go to transportLogs, with the logger name
// "<node>.transport".
func (certificates tlsCertificates) clusterOptions(transportLogs *logBuffer) Options {
	return Options{NewTransport: func(nodeID, bindAddress string) (*raft.NetworkTransport, error) {
		logger := hclog.New(&hclog.LoggerOptions{Name: nodeID + ".transport", Level: hclog.Debug, Output: transportLogs})
		return certificates.newTransport(nodeID, bindAddress, logger)
	}}
}

// tlsLogHasLine reports whether one line of logs contains every part.
func tlsLogHasLine(logs string, parts ...string) bool {
	for line := range strings.SplitSeq(logs, "\n") {
		matches := true
		for _, part := range parts {
			if !strings.Contains(line, part) {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

// Scenario: 3 nodes run Raft over mutual-TLS stream layers, with certificates
// from tlsstream.GenerateCertificates (an IP SAN for 127.0.0.1).
// Expected: one ready leader, a write replicates to all 3 nodes, and the
// leader's Raft port answers a TLS handshake with the leader's certificate.
// Raft lesson: NetworkTransport sends its msgpack RPCs over any
// raft.StreamLayer. A TLS stream layer secures every RPC (AppendEntries,
// votes, pre-votes, snapshots) with no change in raft.
func TestTLSClusterElectsLeaderAndReplicates(t *testing.T) {
	certificates := newTLSCertificates(t)
	cluster := New(t, certificates.clusterOptions(&logBuffer{}))
	leader := cluster.WaitForLeader()
	mustPut(t, leader, "over-tls", "replicated")
	cluster.WaitForValueEverywhere("over-tls", "replicated")

	// Prove that the nodes use the TLS transports, not plain TCP.
	peerConfig, err := certificates.tlsConfig(cluster.Followers(leader.ID())[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: transportTimeout}, "tcp", leader.Status().RaftAddress, peerConfig)
	if err != nil {
		t.Fatalf("TLS handshake with the leader's Raft port: %v", err)
	}
	defer conn.Close()
	if name := conn.ConnectionState().PeerCertificates[0].Subject.CommonName; name != leader.ID() {
		t.Fatalf("the leader's Raft port shows the certificate of %q, want %q", name, leader.ID())
	}
}

// Scenario: a 4th transport has a certificate from another CA. It trusts the
// cluster CA, so only its own certificate is wrong. It sends every node a
// forged heartbeat (AppendEntries) with a term 100 higher than the cluster
// term.
// Expected: every RPC fails. Every node's transport logs that it rejected a
// certificate from an unknown authority. No node takes the forged term or
// names the intruder as leader.
// Raft lesson: Raft RPCs carry no authentication. raft accepts the term and
// the leader of any AppendEntries with a higher term, so without mutual TLS
// one forged heartbeat makes the leader step down. The TLS layer must reject
// the peer before raft reads the request.
func TestTLSPeerWithCertificateFromAnotherCACannotSendRPCs(t *testing.T) {
	certificates := newTLSCertificates(t)
	transportLogs := &logBuffer{}
	cluster := New(t, certificates.clusterOptions(transportLogs))
	leader := cluster.WaitForLeader()
	forgedTerm := leader.Status().Term + 100

	intruder, err := certificates.newTransport(tlsIntruderID, "127.0.0.1:0", hclog.NewNullLogger())
	if err != nil {
		t.Fatalf("open the intruder transport: %v", err)
	}
	t.Cleanup(func() { _ = intruder.Close() })
	for _, member := range cluster.Nodes() {
		// The heartbeat form: no entries, no previous entry, no commit index.
		forgedHeartbeat := &raft.AppendEntriesRequest{
			RPCHeader: raft.RPCHeader{
				ProtocolVersion: raft.ProtocolVersionMax,
				ID:              []byte(tlsIntruderID),
				Addr:            []byte(intruder.LocalAddr()),
			},
			Term: forgedTerm,
		}
		var response raft.AppendEntriesResponse
		err := intruder.AppendEntries(raft.ServerID(member.ID()), raft.ServerAddress(member.Status().RaftAddress), forgedHeartbeat, &response)
		if err == nil {
			t.Fatalf("forged AppendEntries to %s succeeded (response term %d), want a TLS error", member.ID(), response.Term)
		}
		t.Logf("forged AppendEntries to %s: %v", member.ID(), err)
	}

	for _, member := range cluster.Nodes() {
		nodeID := member.ID()
		WaitUntil(t, tlsLogWaitTimeout, nodeID+" transport logs a rejected certificate from an unknown authority", func() bool {
			return tlsLogHasLine(transportLogs.String(), nodeID+".transport:", tlsUnknownAuthorityText)
		})
		if status := member.Status(); status.Term >= forgedTerm || status.LeaderID == tlsIntruderID {
			t.Fatalf("%s has term %d and leader %q after the forged heartbeat (forged term %d)", nodeID, status.Term, status.LeaderID, forgedTerm)
		}
	}
}

// Scenario: a 4th node starts with a certificate from another CA (it trusts
// the cluster CA). The leader adds it as a non-voter, as an operator would by
// mistake, and then writes a key.
// Expected: the configuration change and the write commit with the 3 voters.
// The leader's TLS handshake with the 4th node fails, because its server
// certificate is from an unknown authority. The 4th node gets no log entry,
// no key and no leader.
// Raft lesson: a server in the configuration is not yet a member that gets
// data. Mutual TLS checks the peer in both directions, so a node without a
// certificate from the cluster CA never receives the replicated log.
func TestTLSPeerWithCertificateFromAnotherCACannotJoin(t *testing.T) {
	certificates := newTLSCertificates(t)
	cluster := New(t, certificates.clusterOptions(&logBuffer{}))
	leader := cluster.WaitForLeader()
	clusterIDs := cluster.IDs()

	intruder := cluster.AddNode(tlsIntruderID)
	intruderAddress := intruder.Status().RaftAddress
	if err := leader.AddNonvoter(context.Background(), tlsIntruderID, intruderAddress); err != nil {
		t.Fatalf("AddNonvoter(%s, %s): %v", tlsIntruderID, intruderAddress, err)
	}
	mustPut(t, leader, "after-join", "kept-inside")
	WaitUntil(t, tlsLogWaitTimeout, fmt.Sprintf("after-join on %v", clusterIDs), func() bool {
		for _, id := range clusterIDs {
			result, err := cluster.Node(id).Get(context.Background(), "after-join", node.ReadOptions{Consistency: node.Stale})
			if err != nil || !result.Found {
				return false
			}
		}
		return true
	})
	WaitUntil(t, tlsLogWaitTimeout, "the leader logs a failed TLS handshake with the intruder", func() bool {
		return tlsLogHasLine(cluster.Logs(leader.ID()), intruderAddress, tlsUnknownAuthorityText)
	})

	if status := intruder.Status(); status.LastLogIndex != 0 || status.KeyCount != 0 || status.LeaderID != "" {
		t.Fatalf("intruder status %+v: want no log entry, no key and no known leader", status)
	}
}

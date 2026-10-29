package main

import (
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/tlsstream"
)

func TestCertificateIdentitiesUseTheHostOfEachPeerAddress(t *testing.T) {
	identities, err := certificateIdentities(" node1=172.28.2.11:7000 , node2=raft2.internal:7000")
	if err != nil {
		t.Fatalf("certificateIdentities: %v", err)
	}
	if len(identities) != 2 {
		t.Fatalf("got %d identities, want 2: %+v", len(identities), identities)
	}
	first, second := identities[0], identities[1]
	if first.Name != "node1" || len(first.IPAddresses) != 1 || !first.IPAddresses[0].Equal(net.ParseIP("172.28.2.11")) || len(first.DNSNames) != 0 {
		t.Errorf("node1 identity = %+v, want one IP SAN 172.28.2.11", first)
	}
	if second.Name != "node2" || !slices.Equal(second.DNSNames, []string{"raft2.internal"}) || len(second.IPAddresses) != 0 {
		t.Errorf("node2 identity = %+v, want one DNS SAN raft2.internal", second)
	}
}

func TestCertificateIdentitiesRejectBadPeerLists(t *testing.T) {
	for _, peers := range []string{
		"",
		"node1",
		"node1=172.28.2.11",
		"=172.28.2.11:7000",
		"node1=:7000",
		"node1=172.28.2.11:7000,node1=172.28.2.12:7000",
	} {
		if identities, err := certificateIdentities(peers); err == nil {
			t.Errorf("certificateIdentities(%q) = %+v, want an error", peers, identities)
		}
	}
}

func TestGencertsWritesTheCAAndAKeyPairPerPeer(t *testing.T) {
	certificateDir := t.TempDir()
	err := runGencerts([]string{"-dir", certificateDir, "-peers", "node1=127.0.0.1:7000,node2=127.0.0.2:7000"})
	if err != nil {
		t.Fatalf("runGencerts: %v", err)
	}
	caFile := filepath.Join(certificateDir, tlsstream.CACertificateFile)
	for _, name := range []string{"node1", "node2"} {
		certFile, keyFile := tlsstream.NodeCertificateFiles(certificateDir, name)
		if _, err := tlsstream.LoadMutualTLS(caFile, certFile, keyFile); err != nil {
			t.Errorf("LoadMutualTLS for %s: %v", name, err)
		}
	}
	certFile, _ := tlsstream.NodeCertificateFiles(certificateDir, "node2")
	if certificate := readCertificate(t, certFile); len(certificate.IPAddresses) != 1 || !certificate.IPAddresses[0].Equal(net.ParseIP("127.0.0.2")) {
		t.Errorf("node2 certificate IP SANs = %v, want [127.0.0.2]", certificate.IPAddresses)
	}
}

func TestGencertsReadsThePeersFromKVPeers(t *testing.T) {
	certificateDir := t.TempDir()
	t.Setenv("KV_PEERS", "node3=127.0.0.3:7000")
	if err := runGencerts([]string{"-dir", certificateDir}); err != nil {
		t.Fatalf("runGencerts: %v", err)
	}
	certFile, _ := tlsstream.NodeCertificateFiles(certificateDir, "node3")
	if _, err := os.Stat(certFile); err != nil {
		t.Errorf("node3 certificate: %v", err)
	}
}

func readCertificate(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	block, _ := pem.Decode(encoded)
	if block == nil {
		t.Fatalf("%s has no PEM block", path)
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return certificate
}

// The Docker init containers write the CA dir and copy one node's files into
// that node's own volume.
func TestGencertsExportsOneNodeIntoItsOwnDir(t *testing.T) {
	caDir, exportDir := t.TempDir(), t.TempDir()
	err := runGencerts([]string{"-dir", caDir, "-peers", "node1=127.0.0.1:7000,node2=127.0.0.2:7000", "-export-node", "node2", "-export-dir", exportDir})
	if err != nil {
		t.Fatalf("runGencerts: %v", err)
	}
	entries, err := os.ReadDir(exportDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if want := []string{"ca.crt", "node2.crt", "node2.key"}; !slices.Equal(names, want) {
		t.Fatalf("export dir has %v, want exactly %v", names, want)
	}
}

func TestGencertsRejectsABadExport(t *testing.T) {
	peers := "node1=127.0.0.1:7000"
	for name, exportArguments := range map[string][]string{
		"node without dir":  {"-export-node", "node1"},
		"dir without node":  {"-export-dir", t.TempDir()},
		"node not in peers": {"-export-node", "node9", "-export-dir", t.TempDir()},
	} {
		t.Run(name, func(t *testing.T) {
			arguments := append([]string{"-dir", t.TempDir(), "-peers", peers}, exportArguments...)
			if err := runGencerts(arguments); err == nil {
				t.Fatalf("runGencerts(%v) = nil, want an error", arguments)
			}
		})
	}
}

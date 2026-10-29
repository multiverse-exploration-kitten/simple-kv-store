package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/tlsstream"
)

// runGencerts creates the CA and one certificate per peer. The peer list has
// the KV_PEERS format, so the init container and the nodes share one list.
// A second run keeps the files that exist.
func runGencerts(arguments []string) error {
	flags := flag.NewFlagSet("gencerts", flag.ContinueOnError)
	certificateDir := flags.String("dir", "/certs", "directory for ca.crt and the <id>.crt and <id>.key files")
	peers := flags.String("peers", os.Getenv("KV_PEERS"), "id=host:port,... (default: $KV_PEERS)")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	identities, err := certificateIdentities(*peers)
	if err != nil {
		return err
	}
	if err := tlsstream.GenerateCertificates(*certificateDir, identities); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "certificates for %d nodes are in %s\n", len(identities), *certificateDir)
	return nil
}

// certificateIdentities turns "id=host:port,..." into one identity per peer.
// The host of the address is the SAN that the other peers check when they
// dial: an IP SAN for an IP, a DNS SAN for a name.
func certificateIdentities(peers string) ([]tlsstream.NodeIdentity, error) {
	var identities []tlsstream.NodeIdentity
	seenIDs := map[string]bool{}
	for item := range strings.SplitSeq(peers, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		id, address, _ := strings.Cut(item, "=")
		id, address = strings.TrimSpace(id), strings.TrimSpace(address)
		host, _, err := net.SplitHostPort(address)
		if id == "" || err != nil || host == "" {
			return nil, fmt.Errorf("peers: want id=host:port, got %q", item)
		}
		if seenIDs[id] {
			return nil, fmt.Errorf("peers: duplicate ID %s", id)
		}
		seenIDs[id] = true
		identity := tlsstream.NodeIdentity{Name: id}
		if ip := net.ParseIP(host); ip != nil {
			identity.IPAddresses = []net.IP{ip}
		} else {
			identity.DNSNames = []string{host}
		}
		identities = append(identities, identity)
	}
	if len(identities) == 0 {
		return nil, errors.New("peers: no peers given (set -peers or KV_PEERS)")
	}
	return identities, nil
}

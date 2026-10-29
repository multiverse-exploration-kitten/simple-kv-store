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
// A second run keeps the files that exist. With -export-node it then copies
// ca.crt and that node's key pair to -export-dir: the node's own volume,
// without the CA key and the keys of the other nodes.
func runGencerts(arguments []string) error {
	flags := flag.NewFlagSet("gencerts", flag.ContinueOnError)
	certificateDir := flags.String("dir", "/certs", "directory for ca.crt, ca.key and the <id>.crt and <id>.key files")
	peers := flags.String("peers", os.Getenv("KV_PEERS"), "id=host:port,... (default: $KV_PEERS)")
	exportNode := flags.String("export-node", "", "copy ca.crt and the key pair of this node ID to -export-dir")
	exportDir := flags.String("export-dir", "", "directory for the files of -export-node")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if (*exportNode == "") != (*exportDir == "") {
		return errors.New("-export-node and -export-dir go together")
	}
	identities, err := certificateIdentities(*peers)
	if err != nil {
		return err
	}
	if err := tlsstream.GenerateCertificates(*certificateDir, identities); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "certificates for %d nodes are in %s\n", len(identities), *certificateDir)
	if *exportNode == "" {
		return nil
	}
	if err := tlsstream.ExportNodeCertificates(*certificateDir, *exportNode, *exportDir); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "the files of %s are in %s\n", *exportNode, *exportDir)
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

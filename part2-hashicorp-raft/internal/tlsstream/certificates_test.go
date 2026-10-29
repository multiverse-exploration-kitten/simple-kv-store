package tlsstream_test

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/tlsstream"
)

var loopbackIP = net.IPv4(127, 0, 0, 1)

func loopbackIdentity(name string) tlsstream.NodeIdentity {
	return tlsstream.NodeIdentity{Name: name, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{loopbackIP}}
}

func generate(t *testing.T, dir string, nodes ...tlsstream.NodeIdentity) {
	t.Helper()
	if err := tlsstream.GenerateCertificates(dir, nodes); err != nil {
		t.Fatalf("GenerateCertificates(%s): %v", dir, err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func parseCertificateFile(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(readFile(t, path))
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("%s: no PEM CERTIFICATE block", path)
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return certificate
}

func caPath(dir string) string { return filepath.Join(dir, tlsstream.CACertificateFile) }

// verifyNodeCertificate checks that the node certificate chains to the CA of
// dir for the given key usage.
func verifyNodeCertificate(t *testing.T, dir, name string, usage x509.ExtKeyUsage) error {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(parseCertificateFile(t, caPath(dir)))
	certFile, _ := tlsstream.NodeCertificateFiles(dir, name)
	_, err := parseCertificateFile(t, certFile).Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{usage}})
	return err
}

func TestGenerateCertificatesWritesACAAndOneKeyPairPerNode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "certs")
	generate(t, dir,
		tlsstream.NodeIdentity{Name: "node1", DNSNames: []string{"node1"}, IPAddresses: []net.IP{net.ParseIP("172.28.2.11")}},
		loopbackIdentity("node2"),
	)

	authority := parseCertificateFile(t, caPath(dir))
	if !authority.IsCA || authority.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Fatalf("CA certificate: IsCA=%v KeyUsage=%v, want a CA that can sign certificates", authority.IsCA, authority.KeyUsage)
	}
	certFile, keyFile := tlsstream.NodeCertificateFiles(dir, "node1")
	nodeCertificate := parseCertificateFile(t, certFile)
	if !slices.Equal(nodeCertificate.DNSNames, []string{"node1"}) || len(nodeCertificate.IPAddresses) != 1 ||
		!nodeCertificate.IPAddresses[0].Equal(net.ParseIP("172.28.2.11")) {
		t.Fatalf("node1 SANs: DNS %v, IP %v, want [node1] and [172.28.2.11]", nodeCertificate.DNSNames, nodeCertificate.IPAddresses)
	}
	if nodeCertificate.IsCA {
		t.Fatal("node1 certificate is a CA")
	}
	// A node is a TLS server for its peers and a TLS client when it dials them.
	for _, usage := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if err := verifyNodeCertificate(t, dir, "node1", usage); err != nil {
			t.Fatalf("node1 certificate for usage %v: %v", usage, err)
		}
	}

	for _, privateKeyFile := range []string{filepath.Join(dir, "ca.key"), keyFile} {
		info, err := os.Stat(privateKeyFile)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s has mode %v, want 0600", privateKeyFile, info.Mode().Perm())
		}
	}
	if _, err := tlsstream.LoadMutualTLS(caPath(dir), certFile, keyFile); err != nil {
		t.Fatalf("LoadMutualTLS on the generated files: %v", err)
	}
}

func TestGenerateCertificatesTwiceKeepsTheCAAndExistingNodes(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"))
	node1CertFile, node1KeyFile := tlsstream.NodeCertificateFiles(dir, "node1")
	before := [][]byte{
		readFile(t, caPath(dir)), readFile(t, filepath.Join(dir, "ca.key")),
		readFile(t, node1CertFile), readFile(t, node1KeyFile),
	}

	generate(t, dir, loopbackIdentity("node1"), loopbackIdentity("node2"))

	after := [][]byte{
		readFile(t, caPath(dir)), readFile(t, filepath.Join(dir, "ca.key")),
		readFile(t, node1CertFile), readFile(t, node1KeyFile),
	}
	for index := range before {
		if !bytes.Equal(before[index], after[index]) {
			t.Fatalf("file %d changed on the second run: existing files must stay", index)
		}
	}
	if err := verifyNodeCertificate(t, dir, "node2", x509.ExtKeyUsageServerAuth); err != nil {
		t.Fatalf("node2, added on the second run, is not signed by the kept CA: %v", err)
	}
}

func TestGenerateCertificatesReissuesNodeCertificatesWhenTheCAIsNew(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"))
	if err := os.Remove(caPath(dir)); err != nil {
		t.Fatal(err)
	}

	generate(t, dir, loopbackIdentity("node1"))

	if err := verifyNodeCertificate(t, dir, "node1", x509.ExtKeyUsageServerAuth); err != nil {
		t.Fatalf("node1 after a new CA: %v (an old node certificate would not chain to the new CA)", err)
	}
}

func TestGenerateCertificatesRejectsBadIdentities(t *testing.T) {
	cases := map[string]tlsstream.NodeIdentity{
		"empty name":        {IPAddresses: []net.IP{loopbackIP}},
		"name of the CA":    {Name: "ca", IPAddresses: []net.IP{loopbackIP}},
		"CA in upper case":  {Name: "CA", IPAddresses: []net.IP{loopbackIP}},
		"path in the name":  {Name: "../node1", IPAddresses: []net.IP{loopbackIP}},
		"no DNS name or IP": {Name: "node1"},
	}
	for description, identity := range cases {
		t.Run(description, func(t *testing.T) {
			if err := tlsstream.GenerateCertificates(t.TempDir(), []tlsstream.NodeIdentity{identity}); err == nil {
				t.Fatalf("GenerateCertificates(%+v) = nil, want an error", identity)
			}
		})
	}
}

// A node volume must hold only what the node needs: no CA key and no key of
// another node.
func TestExportNodeCertificatesCopiesOnlyWhatTheNodeNeeds(t *testing.T) {
	caDir, exportDir := t.TempDir(), t.TempDir()
	generate(t, caDir, loopbackIdentity("node1"), loopbackIdentity("node2"))
	if err := tlsstream.ExportNodeCertificates(caDir, "node2", exportDir); err != nil {
		t.Fatalf("ExportNodeCertificates: %v", err)
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
	certFile, keyFile := tlsstream.NodeCertificateFiles(caDir, "node2")
	exportCertFile, exportKeyFile := tlsstream.NodeCertificateFiles(exportDir, "node2")
	for from, to := range map[string]string{caPath(caDir): caPath(exportDir), certFile: exportCertFile, keyFile: exportKeyFile} {
		if !bytes.Equal(readFile(t, from), readFile(t, to)) {
			t.Errorf("%s differs from %s", to, from)
		}
	}
	for path, wantMode := range map[string]os.FileMode{caPath(exportDir): 0o644, exportCertFile: 0o644, exportKeyFile: 0o600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != wantMode {
			t.Errorf("%s has mode %o, want %o", path, info.Mode().Perm(), wantMode)
		}
	}
	if _, err := tlsstream.LoadMutualTLS(caPath(exportDir), exportCertFile, exportKeyFile); err != nil {
		t.Fatalf("LoadMutualTLS from the export dir: %v", err)
	}
}

// A second run replaces the files, so the node volume follows the CA dir
// after a certificate rotation.
func TestExportNodeCertificatesTwiceReplacesTheFiles(t *testing.T) {
	caDir, exportDir := t.TempDir(), t.TempDir()
	generate(t, caDir, loopbackIdentity("node1"))
	if err := tlsstream.ExportNodeCertificates(caDir, "node1", exportDir); err != nil {
		t.Fatalf("first export: %v", err)
	}
	certFile, _ := tlsstream.NodeCertificateFiles(caDir, "node1")
	if err := os.Remove(certFile); err != nil {
		t.Fatal(err)
	}
	generate(t, caDir, loopbackIdentity("node1")) // rotates node1
	if err := tlsstream.ExportNodeCertificates(caDir, "node1", exportDir); err != nil {
		t.Fatalf("second export: %v", err)
	}
	exportCertFile, _ := tlsstream.NodeCertificateFiles(exportDir, "node1")
	if !bytes.Equal(readFile(t, certFile), readFile(t, exportCertFile)) {
		t.Fatal("the second export kept the old certificate")
	}
}

func TestExportNodeCertificatesOfAnUnknownNodeFails(t *testing.T) {
	caDir, exportDir := t.TempDir(), t.TempDir()
	generate(t, caDir, loopbackIdentity("node1"))
	if err := tlsstream.ExportNodeCertificates(caDir, "node9", exportDir); err == nil {
		t.Fatal("ExportNodeCertificates(node9) = nil, want an error: node9 has no key pair")
	}
}

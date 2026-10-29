package tlsstream

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// GenerateCertificates writes these files in its dir:
//
//	ca.crt, ca.key           the cluster CA
//	<name>.crt, <name>.key   one key pair per node
//
// Key files have mode 0600, certificate files 0644.
const (
	CACertificateFile = "ca.crt"
	caKeyFile         = "ca.key"
)

const (
	caValidity   = 10 * 365 * 24 * time.Hour
	nodeValidity = 2 * 365 * 24 * time.Hour
	// NotBefore is this much in the past, so a peer with a slow clock accepts
	// a new certificate.
	clockSkewAllowance = time.Hour
	caCommonName       = "simple-kv-store raft CA"
)

var nodeNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// NodeIdentity is the subject of one node certificate. The certificate must
// name the host part of every address that peers dial: an IP SAN for an IP
// address, a DNS SAN for a host name.
type NodeIdentity struct {
	// Name is the common name and the file name, for example "node1".
	Name        string
	DNSNames    []string
	IPAddresses []net.IP
}

// NodeCertificateFiles returns the paths of the certificate and key that
// GenerateCertificates writes for the node called name.
func NodeCertificateFiles(dir, name string) (certFile, keyFile string) {
	return filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
}

// GenerateCertificates creates a CA and one certificate per node in dir. Each
// node certificate is good for TLS server and TLS client authentication.
//
// It keeps the files that exist, so a second run (a restarted init
// container) does not change the CA. It adds certificates for new nodes. When
// it must create a new CA, it issues new certificates for all nodes. The
// certificate file marks a complete key pair: the key file is written first,
// and every file is written to a temporary file and renamed.
func GenerateCertificates(dir string, nodes []NodeIdentity) error {
	for _, identity := range nodes {
		if err := validateIdentity(identity); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("tlsstream: create certificate dir: %w", err)
	}
	authority, createdAuthority, err := loadOrCreateAuthority(dir)
	if err != nil {
		return err
	}
	for _, identity := range nodes {
		certFile, keyFile := NodeCertificateFiles(dir, identity.Name)
		exists, err := fileExists(certFile)
		if err != nil {
			return err
		}
		if exists && !createdAuthority {
			continue
		}
		if err := authority.issueNodeCertificate(identity, certFile, keyFile); err != nil {
			return fmt.Errorf("tlsstream: issue certificate for %s: %w", identity.Name, err)
		}
	}
	return nil
}

func validateIdentity(identity NodeIdentity) error {
	switch {
	case !nodeNamePattern.MatchString(identity.Name):
		return fmt.Errorf("tlsstream: node name %q must match %s", identity.Name, nodeNamePattern)
	// EqualFold: on a case-insensitive file system "CA.crt" is "ca.crt".
	case strings.EqualFold(identity.Name, "ca"):
		return fmt.Errorf("tlsstream: node name %q is used by the CA files", identity.Name)
	case len(identity.DNSNames) == 0 && len(identity.IPAddresses) == 0:
		return fmt.Errorf("tlsstream: node %s needs at least one DNS name or IP address", identity.Name)
	}
	return nil
}

type certificateAuthority struct {
	certificate *x509.Certificate
	privateKey  crypto.Signer
}

func loadOrCreateAuthority(dir string) (authority certificateAuthority, created bool, err error) {
	certFile, keyFile := filepath.Join(dir, CACertificateFile), filepath.Join(dir, caKeyFile)
	exists, err := fileExists(certFile)
	if err != nil {
		return certificateAuthority{}, false, err
	}
	if exists {
		authority, err = loadAuthority(certFile, keyFile)
		return authority, false, err
	}
	authority, err = createAuthority(certFile, keyFile)
	return authority, true, err
}

func loadAuthority(certFile, keyFile string) (certificateAuthority, error) {
	certificateDER, err := readPEM(certFile, "CERTIFICATE")
	if err != nil {
		return certificateAuthority{}, err
	}
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		return certificateAuthority{}, fmt.Errorf("tlsstream: parse %s: %w", certFile, err)
	}
	keyDER, err := readPEM(keyFile, "PRIVATE KEY")
	if err != nil {
		return certificateAuthority{}, err
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return certificateAuthority{}, fmt.Errorf("tlsstream: parse %s: %w", keyFile, err)
	}
	signer, isSigner := parsedKey.(crypto.Signer)
	if !isSigner {
		return certificateAuthority{}, fmt.Errorf("tlsstream: %s does not hold a signing key", keyFile)
	}
	return certificateAuthority{certificate: certificate, privateKey: signer}, nil
}

func createAuthority(certFile, keyFile string) (certificateAuthority, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return certificateAuthority{}, err
	}
	serialNumber, err := randomSerialNumber()
	if err != nil {
		return certificateAuthority{}, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{CommonName: caCommonName},
		NotBefore:             now.Add(-clockSkewAllowance),
		NotAfter:              now.Add(caValidity),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return certificateAuthority{}, fmt.Errorf("tlsstream: create CA certificate: %w", err)
	}
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		return certificateAuthority{}, err
	}
	if err := writeKeyPair(certFile, keyFile, certificateDER, privateKey); err != nil {
		return certificateAuthority{}, err
	}
	return certificateAuthority{certificate: certificate, privateKey: privateKey}, nil
}

func (authority certificateAuthority) issueNodeCertificate(identity NodeIdentity, certFile, keyFile string) error {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serialNumber, err := randomSerialNumber()
	if err != nil {
		return err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: identity.Name},
		NotBefore:    now.Add(-clockSkewAllowance),
		NotAfter:     now.Add(nodeValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		// A node is a TLS server for the peers that dial it and a TLS client
		// when it dials them.
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    identity.DNSNames,
		IPAddresses: identity.IPAddresses,
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, authority.certificate, &privateKey.PublicKey, authority.privateKey)
	if err != nil {
		return err
	}
	return writeKeyPair(certFile, keyFile, certificateDER, privateKey)
}

func randomSerialNumber() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

// writeKeyPair writes the key before the certificate: an existing
// certificate file means that the key file is complete.
func writeKeyPair(certFile, keyFile string, certificateDER []byte, privateKey *ecdsa.PrivateKey) error {
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return err
	}
	if err := writePEMFile(keyFile, "PRIVATE KEY", keyDER, 0o600); err != nil {
		return err
	}
	return writePEMFile(certFile, "CERTIFICATE", certificateDER, 0o644)
}

// writePEMFile writes a temporary file, syncs it and renames it, so a crash
// never leaves a half-written file at path.
func writePEMFile(path, blockType string, der []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".tmp-"+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("tlsstream: write %s: %w", path, err)
	}
	defer os.Remove(temporary.Name()) // fails harmlessly after the rename
	writeErr := pem.Encode(temporary, &pem.Block{Type: blockType, Bytes: der})
	if writeErr == nil {
		writeErr = temporary.Chmod(mode)
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	if closeErr := temporary.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(temporary.Name(), path)
	}
	if writeErr != nil {
		return fmt.Errorf("tlsstream: write %s: %w", path, writeErr)
	}
	return nil
}

func readPEM(path, blockType string) ([]byte, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tlsstream: read %s: %w", path, err)
	}
	block, _ := pem.Decode(content)
	if block == nil || block.Type != blockType {
		return nil, fmt.Errorf("tlsstream: %s has no PEM %s block", path, blockType)
	}
	return block.Bytes, nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("tlsstream: check %s: %w", path, err)
	}
	return true, nil
}

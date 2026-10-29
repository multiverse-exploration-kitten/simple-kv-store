package tlsstream_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/tlsstream"
)

const exchangeTimeout = 5 * time.Second

func loadConfig(t *testing.T, caDir, nodeDir, name string) *tls.Config {
	t.Helper()
	certFile, keyFile := tlsstream.NodeCertificateFiles(nodeDir, name)
	config, err := tlsstream.LoadMutualTLS(filepath.Join(caDir, tlsstream.CACertificateFile), certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadMutualTLS(%s): %v", name, err)
	}
	return config
}

func newLayer(t *testing.T, config *tls.Config) *tlsstream.StreamLayer {
	t.Helper()
	layer, err := tlsstream.NewStreamLayer("127.0.0.1:0", nil, config)
	if err != nil {
		t.Fatalf("NewStreamLayer: %v", err)
	}
	t.Cleanup(func() { _ = layer.Close() })
	return layer
}

type serverResult struct {
	err            error
	peerCommonName string
}

// serveOne accepts one connection, completes the TLS handshake and echoes
// one byte.
func serveOne(layer *tlsstream.StreamLayer) <-chan serverResult {
	results := make(chan serverResult, 1)
	go func() {
		conn, err := layer.Accept()
		if err != nil {
			results <- serverResult{err: err}
			return
		}
		defer conn.Close()
		tlsConn := conn.(*tls.Conn)
		ctx, cancel := context.WithTimeout(context.Background(), exchangeTimeout)
		defer cancel()
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			results <- serverResult{err: err}
			return
		}
		peerCommonName := tlsConn.ConnectionState().PeerCertificates[0].Subject.CommonName
		_ = conn.SetDeadline(time.Now().Add(exchangeTimeout))
		buffer := make([]byte, 1)
		if _, err := io.ReadFull(conn, buffer); err != nil {
			results <- serverResult{err: err, peerCommonName: peerCommonName}
			return
		}
		_, err = conn.Write(buffer)
		results <- serverResult{err: err, peerCommonName: peerCommonName}
	}()
	return results
}

// exchange dials the server, sends one byte and reads the echo. In TLS 1.3
// the client finishes its handshake before the server checks the client
// certificate, so a rejected client sees the error on its first read.
func exchange(client, server *tlsstream.StreamLayer) (serverCommonName string, err error) {
	conn, err := client.Dial(raft.ServerAddress(server.Addr().String()), exchangeTimeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	serverCommonName = conn.(*tls.Conn).ConnectionState().PeerCertificates[0].Subject.CommonName
	_ = conn.SetDeadline(time.Now().Add(exchangeTimeout))
	if _, err := conn.Write([]byte{42}); err != nil {
		return serverCommonName, err
	}
	echo := make([]byte, 1)
	if _, err := io.ReadFull(conn, echo); err != nil {
		return serverCommonName, err
	}
	return serverCommonName, nil
}

func waitForServer(t *testing.T, results <-chan serverResult) serverResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(2 * exchangeTimeout):
		t.Fatal("timed out waiting for the server side of the connection")
		return serverResult{}
	}
}

func TestMutualTLSHandshakeBetweenTwoNodesOfOneCA(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"), loopbackIdentity("node2"))
	server := newLayer(t, loadConfig(t, dir, dir, "node1"))
	client := newLayer(t, loadConfig(t, dir, dir, "node2"))
	results := serveOne(server)

	serverName, err := exchange(client, server)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	result := waitForServer(t, results)
	if result.err != nil {
		t.Fatalf("server: %v", result.err)
	}
	if serverName != "node1" || result.peerCommonName != "node2" {
		t.Fatalf("client saw server %q, server saw client %q: want node1 and node2", serverName, result.peerCommonName)
	}
}

// The client trusts the cluster CA, so only its own certificate is wrong.
func TestClientWithCertificateFromAnotherCAIsRejected(t *testing.T) {
	clusterDir, otherDir := t.TempDir(), t.TempDir()
	generate(t, clusterDir, loopbackIdentity("node1"))
	generate(t, otherDir, loopbackIdentity("intruder"))
	server := newLayer(t, loadConfig(t, clusterDir, clusterDir, "node1"))
	client := newLayer(t, loadConfig(t, clusterDir, otherDir, "intruder"))
	results := serveOne(server)

	_, clientErr := exchange(client, server)
	result := waitForServer(t, results)
	var unknownAuthority x509.UnknownAuthorityError
	if !errors.As(result.err, &unknownAuthority) {
		t.Fatalf("server error = %v, want x509.UnknownAuthorityError", result.err)
	}
	if clientErr == nil {
		t.Fatal("client exchange succeeded, want an error")
	}
}

func TestClientWithoutCertificateIsRejected(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"), loopbackIdentity("node2"))
	server := newLayer(t, loadConfig(t, dir, dir, "node1"))
	noCertificate := loadConfig(t, dir, dir, "node2")
	noCertificate.Certificates = nil
	client := newLayer(t, noCertificate)
	results := serveOne(server)

	_, clientErr := exchange(client, server)
	result := waitForServer(t, results)
	if result.err == nil || !strings.Contains(result.err.Error(), "didn't provide a certificate") {
		t.Fatalf("server error = %v, want \"client didn't provide a certificate\"", result.err)
	}
	if clientErr == nil || !strings.Contains(clientErr.Error(), "certificate required") {
		t.Fatalf("client error = %v, want the alert \"certificate required\"", clientErr)
	}
}

// The server name comes from the dial address. A certificate without that IP
// in its SANs fails, also when the same CA signed it.
func TestServerCertificateForAnotherIPIsRejected(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir,
		tlsstream.NodeIdentity{Name: "elsewhere", DNSNames: []string{"elsewhere"}, IPAddresses: []net.IP{net.ParseIP("10.0.0.7")}},
		loopbackIdentity("node2"),
	)
	server := newLayer(t, loadConfig(t, dir, dir, "elsewhere"))
	client := newLayer(t, loadConfig(t, dir, dir, "node2"))
	results := serveOne(server)

	_, clientErr := exchange(client, server)
	waitForServer(t, results)
	var hostnameError x509.HostnameError
	if !errors.As(clientErr, &hostnameError) {
		t.Fatalf("client error = %v, want x509.HostnameError for 127.0.0.1", clientErr)
	}
}

func TestClientWithTLS12IsRejected(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"), loopbackIdentity("node2"))
	server := newLayer(t, loadConfig(t, dir, dir, "node1"))
	oldClient := loadConfig(t, dir, dir, "node2")
	oldClient.MinVersion, oldClient.MaxVersion = tls.VersionTLS12, tls.VersionTLS12
	client := newLayer(t, oldClient)
	results := serveOne(server)

	_, clientErr := exchange(client, server)
	result := waitForServer(t, results)
	if clientErr == nil || !strings.Contains(clientErr.Error(), "protocol version") {
		t.Fatalf("client error = %v, want the alert \"protocol version not supported\"", clientErr)
	}
	if result.err == nil {
		t.Fatal("server accepted a TLS 1.2 handshake")
	}
}

// A peer that accepts TCP but never answers the handshake must not block Dial
// longer than the timeout that NetworkTransport passes.
func TestDialTimeoutCoversTheTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"))
	client := newLayer(t, loadConfig(t, dir, dir, "node1"))
	silentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = silentListener.Close() })
	go func() {
		var heldConns []net.Conn
		defer func() {
			for _, conn := range heldConns {
				_ = conn.Close()
			}
		}()
		for {
			conn, err := silentListener.Accept()
			if err != nil {
				return
			}
			heldConns = append(heldConns, conn)
		}
	}()

	started := time.Now()
	conn, err := client.Dial(raft.ServerAddress(silentListener.Addr().String()), 200*time.Millisecond)
	elapsed := time.Since(started)
	if err == nil {
		_ = conn.Close()
		t.Fatal("Dial to a silent peer succeeded, want a handshake timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) || elapsed > 2*time.Second {
		t.Fatalf("Dial returned %v after %s, want context.DeadlineExceeded after about 200ms", err, elapsed)
	}
}

func TestNewStreamLayerRequiresVerifiedClientCertificates(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"))
	config := loadConfig(t, dir, dir, "node1")
	config.ClientAuth = tls.VerifyClientCertIfGiven
	if layer, err := tlsstream.NewStreamLayer("127.0.0.1:0", nil, config); err == nil {
		_ = layer.Close()
		t.Fatal("NewStreamLayer accepted a config without RequireAndVerifyClientCert")
	}
}

func TestNewStreamLayerNeedsAnAdvertisableAddress(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"))
	config := loadConfig(t, dir, dir, "node1")
	if layer, err := tlsstream.NewStreamLayer("0.0.0.0:0", nil, config); err == nil {
		_ = layer.Close()
		t.Fatal("NewStreamLayer(0.0.0.0:0, no advertise) = nil error, want \"not advertisable\"")
	}

	advertise := &net.TCPAddr{IP: loopbackIP, Port: 7777}
	layer, err := tlsstream.NewStreamLayer("0.0.0.0:0", advertise, config)
	if err != nil {
		t.Fatalf("NewStreamLayer with advertise %s: %v", advertise, err)
	}
	defer layer.Close()
	if layer.Addr().String() != "127.0.0.1:7777" {
		t.Fatalf("Addr() = %s, want the advertise address 127.0.0.1:7777", layer.Addr())
	}
}

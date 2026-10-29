package tlsstream_test

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// 500 ms: under parallel -race load a test process can pause for 230 ms.
const testHandshakeTimeout = 500 * time.Millisecond

// silentListener accepts TCP connections and never sends a byte. It closes
// them when the test ends.
func silentListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		var heldConns []net.Conn
		defer func() {
			for _, conn := range heldConns {
				_ = conn.Close()
			}
		}()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			heldConns = append(heldConns, conn)
		}
	}()
	return listener
}

// A TCP client that connects to the Raft port and never starts the TLS
// handshake must not keep its connection and a transport goroutine forever.
// The test uses raft's real accept loop (NetworkTransport).
func TestSilentInboundConnectionIsClosedAfterTheHandshakeTimeout(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"))
	layer := newLayer(t, loadConfig(t, dir, dir, "node1"))
	layer.SetHandshakeTimeout(testHandshakeTimeout)
	transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{Stream: layer, MaxPool: 3, Timeout: time.Second})
	t.Cleanup(func() { _ = transport.Close() })

	conn, err := net.Dial("tcp", layer.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const waitLimit = 3 * time.Second
	_ = conn.SetReadDeadline(time.Now().Add(waitLimit))
	started := time.Now()
	_, err = conn.Read(make([]byte, 1))
	if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the connection is still open %s after a connect without a TLS handshake (handshake timeout %s)", waitLimit, testHandshakeTimeout)
	}
	if err == nil {
		t.Fatal("the server sent data to a client that sent nothing")
	}
	t.Logf("the server closed the silent connection after %s: %v", time.Since(started).Round(time.Millisecond), err)
}

// raft accepts all connections in one goroutine. Accept must return a
// connection at once, also when the client does not start the handshake.
func TestAcceptDoesNotWaitForTheHandshake(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"))
	layer := newLayer(t, loadConfig(t, dir, dir, "node1"))

	conn, err := net.Dial("tcp", layer.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	accepted := make(chan error, 1)
	go func() {
		acceptedConn, err := layer.Accept()
		if err == nil {
			_ = acceptedConn.Close()
		}
		accepted <- err
	}()
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("Accept: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return in 2 s for a client that sent no handshake")
	}
}

// The handshake timeout ends with the handshake. A peer connection that is
// idle longer than the timeout after the handshake stays open: raft keeps
// pooled connections idle between RPCs.
func TestIdleConnectionAfterTheHandshakeStaysOpen(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"), loopbackIdentity("node2"))
	server := newLayer(t, loadConfig(t, dir, dir, "node1"))
	server.SetHandshakeTimeout(testHandshakeTimeout)
	client := newLayer(t, loadConfig(t, dir, dir, "node2"))
	results := serveOne(server)

	conn, err := client.Dial(raft.ServerAddress(server.Addr().String()), exchangeTimeout)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	time.Sleep(3 * testHandshakeTimeout) // idle after the handshake, longer than the timeout
	_ = conn.SetDeadline(time.Now().Add(exchangeTimeout))
	if _, err := conn.Write([]byte{42}); err != nil {
		t.Fatalf("write after the idle time: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		t.Fatalf("read the echo after the idle time: %v", err)
	}
	if result := waitForServer(t, results); result.err != nil {
		t.Fatalf("server: %v", result.err)
	}
}

// NetworkTransportConfig.Timeout can be 0 (no I/O deadlines). Dial must still
// end the handshake with a silent peer.
func TestDialWithoutTimeoutStillLimitsTheHandshake(t *testing.T) {
	dir := t.TempDir()
	generate(t, dir, loopbackIdentity("node1"))
	client := newLayer(t, loadConfig(t, dir, dir, "node1"))
	client.SetHandshakeTimeout(testHandshakeTimeout)
	listener := silentListener(t)

	result := make(chan error, 1)
	go func() {
		conn, err := client.Dial(raft.ServerAddress(listener.Addr().String()), 0)
		if err == nil {
			_ = conn.Close()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Dial(timeout 0) to a silent peer = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Dial(timeout 0) to a silent peer did not return in 3 s")
	}
}

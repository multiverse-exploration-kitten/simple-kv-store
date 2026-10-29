// Package tlsstream secures Raft traffic between peers with mutual TLS.
//
// Raft RPCs carry no authentication: any host that reaches the Raft port can
// send AppendEntries. StreamLayer is a raft.StreamLayer for
// raft.NewNetworkTransportWithConfig that accepts and dials TLS 1.3 only, and
// both sides must show a certificate from the cluster CA.
package tlsstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// defaultHandshakeTimeout limits the TLS handshake of an inbound connection
// (Consul's rpc_handshake_timeout). NetworkTransport sets no deadline on
// inbound connections, so without it a client that connects and stays
// silent keeps a socket and a goroutine forever.
const defaultHandshakeTimeout = 5 * time.Second

// StreamLayer listens and dials with one mutual-TLS config. Each node is a
// TLS server for the peers that dial it and a TLS client when it dials them.
type StreamLayer struct {
	listener         net.Listener
	advertise        net.Addr
	tlsConfig        *tls.Config
	handshakeTimeout time.Duration
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// NewStreamLayer binds bindAddress. tlsConfig must require and verify client
// certificates (see LoadMutualTLS). advertise is the address that peers dial.
// When it is nil, the bound address is used. Like raft.NewTCPTransport, the
// address must be a TCP address with a specified IP.
func NewStreamLayer(bindAddress string, advertise net.Addr, tlsConfig *tls.Config) (*StreamLayer, error) {
	if tlsConfig == nil || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		return nil, errors.New("tlsstream: the TLS config must set ClientAuth to RequireAndVerifyClientCert")
	}
	tcpListener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}
	layer := &StreamLayer{
		listener:         tcpListener,
		advertise:        advertise,
		tlsConfig:        tlsConfig,
		handshakeTimeout: defaultHandshakeTimeout,
	}
	if tcpAddress, isTCP := layer.Addr().(*net.TCPAddr); !isTCP || tcpAddress.IP == nil || tcpAddress.IP.IsUnspecified() {
		_ = tcpListener.Close()
		return nil, fmt.Errorf("tlsstream: address %s is not advertisable: give an advertise address with an IP", layer.Addr())
	}
	return layer, nil
}

// Accept returns a TLS server connection. The handshake runs on the first
// Read or Write, in the connection goroutine of NetworkTransport, so a slow
// peer does not block the accept loop. A handshake that does not finish
// within the handshake timeout closes the connection.
func (layer *StreamLayer) Accept() (net.Conn, error) {
	conn, err := layer.listener.Accept()
	if err != nil {
		return nil, err
	}
	return &inboundConn{Conn: tls.Server(conn, layer.tlsConfig), handshakeTimeout: layer.handshakeTimeout}, nil
}

// Close stops the listener. NetworkTransport.Close calls it.
func (layer *StreamLayer) Close() error { return layer.listener.Close() }

// Addr is the advertise address, or the bound address. raft uses it as
// LocalAddr.
func (layer *StreamLayer) Addr() net.Addr {
	if layer.advertise != nil {
		return layer.advertise
	}
	return layer.listener.Addr()
}

// Dial opens a TCP connection and completes the TLS handshake. The timeout
// covers the connect and the handshake together. A timeout of 0 (raft's
// "no I/O deadlines") uses the handshake timeout. The TLS server name is the
// host of the dial address, so a peer certificate needs an IP SAN for an IP
// address and a DNS SAN for a host name.
func (layer *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	host, _, err := net.SplitHostPort(string(address))
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = layer.handshakeTimeout
	}
	config := layer.tlsConfig.Clone()
	config.ServerName = host
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", string(address), config)
}

// LoadMutualTLS reads PEM files and returns a config for NewStreamLayer:
// TLS 1.3 minimum, this node's certificate for both roles, and the CA as the
// only trusted root for server and client certificates. ServerName stays
// empty: Dial sets it from each dial address.
func LoadMutualTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsstream: load key pair %s and %s: %w", certFile, keyFile, err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tlsstream: read CA file: %w", err)
	}
	authorities := x509.NewCertPool()
	if !authorities.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("tlsstream: no PEM certificate in CA file %s", caFile)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      authorities, // checks the peer when this node dials
		ClientCAs:    authorities, // checks the peer that dials this node
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}

// inboundConn runs the server handshake on the first Read or Write, under
// handshakeTimeout. HandshakeContext closes the connection when the
// deadline passes. After the handshake no deadline is left: raft keeps
// pooled peer connections idle between RPCs.
type inboundConn struct {
	*tls.Conn
	handshakeTimeout time.Duration
	handshakeOnce    sync.Once
	handshakeErr     error
}

func (conn *inboundConn) Read(buffer []byte) (int, error) {
	if err := conn.handshake(); err != nil {
		return 0, err
	}
	return conn.Conn.Read(buffer)
}

func (conn *inboundConn) Write(buffer []byte) (int, error) {
	if err := conn.handshake(); err != nil {
		return 0, err
	}
	return conn.Conn.Write(buffer)
}

func (conn *inboundConn) handshake() error {
	conn.handshakeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), conn.handshakeTimeout)
		defer cancel()
		conn.handshakeErr = conn.Conn.HandshakeContext(ctx)
		if errors.Is(conn.handshakeErr, context.DeadlineExceeded) {
			conn.handshakeErr = fmt.Errorf("tlsstream: no TLS handshake from %s within %s: %w", conn.RemoteAddr(), conn.handshakeTimeout, conn.handshakeErr)
		}
	})
	return conn.handshakeErr
}

// Package tlsstream secures Raft traffic between peers with mutual TLS.
//
// Raft RPCs carry no authentication: any host that reaches the Raft port can
// send AppendEntries. StreamLayer is a raft.StreamLayer for
// raft.NewNetworkTransportWithConfig that accepts and dials TLS 1.3 only, and
// both sides must show a certificate from the cluster CA.
package tlsstream

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

// StreamLayer listens and dials with one mutual-TLS config. Each node is a
// TLS server for the peers that dial it and a TLS client when it dials them.
type StreamLayer struct {
	listener  net.Listener
	advertise net.Addr
	tlsConfig *tls.Config
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
		listener:  tls.NewListener(tcpListener, tlsConfig),
		advertise: advertise,
		tlsConfig: tlsConfig,
	}
	if tcpAddress, isTCP := layer.Addr().(*net.TCPAddr); !isTCP || tcpAddress.IP == nil || tcpAddress.IP.IsUnspecified() {
		_ = tcpListener.Close()
		return nil, fmt.Errorf("tlsstream: address %s is not advertisable: give an advertise address with an IP", layer.Addr())
	}
	return layer, nil
}

// Accept returns a *tls.Conn. The handshake runs on the first Read, in the
// connection goroutine of NetworkTransport, so a slow peer does not block the
// accept loop.
func (layer *StreamLayer) Accept() (net.Conn, error) { return layer.listener.Accept() }

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
// covers the connect and the handshake together. The TLS server name is the
// host of the dial address, so a peer certificate needs an IP SAN for an IP
// address and a DNS SAN for a host name.
func (layer *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	host, _, err := net.SplitHostPort(string(address))
	if err != nil {
		return nil, err
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

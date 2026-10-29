package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-metrics"
	metricsprometheus "github.com/hashicorp/go-metrics/prometheus"
	"github.com/hashicorp/raft"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/config"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/httpapi"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/tlsstream"
)

const (
	// Consul's transport values: idle connections kept per peer, and the I/O
	// deadline of one RPC (the follower's fsync is inside it).
	transportMaxPool = 3
	transportTimeout = 10 * time.Second
	// holdTimeout: how long the HTTP API waits for a leader before a 503.
	holdTimeout       = 5 * time.Second
	readHeaderTimeout = 10 * time.Second
	// readTimeout limits reading the whole request: the header and a body
	// of up to about 3 MiB. It does not limit the handler: net/http clears
	// the read deadline when the handler has read the body.
	readTimeout = 30 * time.Second
	// idleTimeout is longer than the 90 s idle timeout of the forward client
	// (http.DefaultTransport). So the client closes an idle connection
	// first, and a forward never goes out on a connection that the server
	// is closing.
	idleTimeout = 2 * time.Minute
)

// runServe runs the node until SIGTERM or SIGINT.
func runServe() error {
	serverConfig, err := config.FromEnvironment()
	if err != nil {
		return err
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "kvstore",
		Level:      hclog.LevelFromString(serverConfig.LogLevel),
		JSONFormat: serverConfig.LogJSON,
		Output:     os.Stderr,
	}).With("node_id", serverConfig.NodeID)
	if err := setGlobalMetricsSink(); err != nil {
		return err
	}
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	if err := serve(ctx, serverConfig, logger); err != nil {
		logger.Error("stopped with an error", "error", err)
		return err
	}
	logger.Info("stopped")
	return nil
}

// setGlobalMetricsSink sends the go-metrics of raft, raft-boltdb and the
// transport to the default Prometheus registry, which /metrics serves. The
// sink is global, so only this process entry point sets it (storage doc Q11).
func setGlobalMetricsSink() error {
	sink, err := metricsprometheus.NewPrometheusSink()
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	metricsConfig := metrics.DefaultConfig("kvstore")
	metricsConfig.EnableHostname = false
	// The default Prometheus registry already has the Go runtime collector.
	metricsConfig.EnableRuntimeMetrics = false
	if _, err := metrics.NewGlobal(metricsConfig, sink); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	return nil
}

// serve starts the transport, the node and the HTTP server, and stops them
// when ctx ends.
func serve(ctx context.Context, serverConfig config.Config, logger hclog.Logger) error {
	transport, err := newTransport(serverConfig, logger)
	if err != nil {
		return err
	}
	kvNode, err := node.New(node.Config{
		NodeID:         serverConfig.NodeID,
		RaftAddress:    serverConfig.RaftAddress,
		DataDir:        serverConfig.DataDir,
		Bootstrap:      serverConfig.Bootstrap,
		InitialServers: serverConfig.Peers,
		Timing:         node.DefaultTiming(serverConfig.TimingMultiplier),
		CommandTimeout: serverConfig.CommandTimeout,
		Logger:         logger,
	}, node.Dependencies{Transport: transport})
	if err != nil {
		return errors.Join(err, transport.Close())
	}

	listener, err := net.Listen("tcp", serverConfig.HTTPAddress)
	if err != nil {
		return errors.Join(fmt.Errorf("http: %w", err), kvNode.Close())
	}
	// The leader's own 504 must arrive before the forward gives up.
	forwardTimeout := 2 * serverConfig.CommandTimeout
	handler := httpapi.New(kvNode, httpapi.PeerDirectory(serverConfig.PeerHTTP), httpapi.Options{
		HoldTimeout:    holdTimeout,
		ForwardTimeout: forwardTimeout,
		AdminToken:     serverConfig.AdminToken,
		Logger:         logger.Named("http"),
	})
	httpServer := newHTTPServer(handler, holdTimeout, forwardTimeout, logger.Named("http"))
	serveResult := make(chan error, 1)
	go func() { serveResult <- httpServer.Serve(listener) }()
	logger.Info("started",
		"raft_address", serverConfig.RaftAddress,
		"http_address", listener.Addr().String(),
		"bootstrap", serverConfig.Bootstrap,
		"raft_tls", !serverConfig.RaftInsecurePlaintext,
		"admin_calls", serverConfig.AdminToken != "")

	select {
	case <-ctx.Done():
	case err := <-serveResult:
		return errors.Join(fmt.Errorf("http: %w", err), kvNode.Close())
	}
	logger.Info("stopping")
	return shutdown(kvNode, httpServer, shutdownTimeouts{
		leadershipTransfer: serverConfig.CommandTimeout,
		// A request can wait for a leader, then wait for the forward.
		httpDrain: holdTimeout + forwardTimeout,
	}, logger)
}

// newHTTPServer limits each step of a request, so a slow or silent client
// cannot keep a connection and a goroutine forever. The write deadline
// starts when the request header is read. It covers the body read (less
// than readTimeout), the hold for a leader, one forward, and the answer.
// Without that margin a held and forwarded request would be cut.
func newHTTPServer(handler http.Handler, holdTimeout, forwardTimeout time.Duration, logger hclog.Logger) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      readTimeout + holdTimeout + forwardTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          logger.StandardLogger(&hclog.StandardLoggerOptions{InferLevels: true}),
	}
}

type shutdownTimeouts struct {
	leadershipTransfer time.Duration
	httpDrain          time.Duration
}

// shutdown hands leadership to another voter first, so the cluster does not
// wait an election timeout for a new leader (raft's Shutdown is not
// graceful, reference Q14). Then it lets the HTTP requests finish. Requests
// that arrive during the transfer wait for the new leader and are forwarded.
// Last, it stops raft and closes the stores and the transport.
func shutdown(kvNode *node.Node, httpServer *http.Server, timeouts shutdownTimeouts, logger hclog.Logger) error {
	if kvNode.IsLeader() {
		transferContext, cancelTransfer := context.WithTimeout(context.Background(), timeouts.leadershipTransfer)
		err := kvNode.TransferLeadership(transferContext, "")
		cancelTransfer()
		if err != nil {
			logger.Warn("leadership transfer failed, stopping anyway", "error", err)
		} else {
			logger.Info("leadership transferred")
		}
	}

	drainContext, cancelDrain := context.WithTimeout(context.Background(), timeouts.httpDrain)
	defer cancelDrain()
	if err := httpServer.Shutdown(drainContext); err != nil {
		logger.Warn("HTTP requests did not finish in time, closing them", "error", err)
		_ = httpServer.Close()
	}
	return kvNode.Close()
}

// newTransport returns the Raft transport: mutual TLS, or plain TCP only
// with KV_RAFT_INSECURE_PLAINTEXT=true. config.Load already checked that
// KV_RAFT_ADDRESS is IP:port.
func newTransport(serverConfig config.Config, logger hclog.Logger) (*raft.NetworkTransport, error) {
	advertise, err := net.ResolveTCPAddr("tcp", serverConfig.RaftAddress)
	if err != nil {
		return nil, fmt.Errorf("KV_RAFT_ADDRESS: %w", err)
	}
	transportConfig := &raft.NetworkTransportConfig{
		MaxPool: transportMaxPool,
		Timeout: transportTimeout,
		Logger:  logger.Named("transport"),
	}
	if serverConfig.RaftInsecurePlaintext {
		logger.Warn("raft traffic is not encrypted and not authenticated (KV_RAFT_INSECURE_PLAINTEXT=true)")
		transport, err := raft.NewTCPTransportWithConfig(serverConfig.RaftBindAddress, advertise, transportConfig)
		if err != nil {
			return nil, fmt.Errorf("raft transport: %w", err)
		}
		return transport, nil
	}
	tlsConfig, err := tlsstream.LoadMutualTLS(serverConfig.RaftTLSCAFile, serverConfig.RaftTLSCertFile, serverConfig.RaftTLSKeyFile)
	if err != nil {
		return nil, err
	}
	streamLayer, err := tlsstream.NewStreamLayer(serverConfig.RaftBindAddress, advertise, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("raft transport: %w", err)
	}
	transportConfig.Stream = streamLayer
	return raft.NewNetworkTransportWithConfig(transportConfig), nil
}

package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

const healthcheckTimeout = 2 * time.Second

// runHealthcheck checks the HTTP API of the node in this container. It reads
// KV_HTTP_ADDRESS only, so a bad value elsewhere does not mark the container
// unhealthy.
func runHealthcheck() error {
	httpAddress := os.Getenv("KV_HTTP_ADDRESS")
	if httpAddress == "" {
		httpAddress = ":8000" // the default of internal/config
	}
	url, err := liveURL(httpAddress)
	if err != nil {
		return err
	}
	return checkLive(&http.Client{Timeout: healthcheckTimeout}, url)
}

// liveURL returns the /health/live URL for a listen address. A wildcard
// host (empty, 0.0.0.0 or ::) is dialed on 127.0.0.1.
func liveURL(httpAddress string) (string, error) {
	host, port, err := net.SplitHostPort(httpAddress)
	if err != nil {
		return "", fmt.Errorf("KV_HTTP_ADDRESS: %w", err)
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/health/live", nil
}

func checkLive(client *http.Client, url string) error {
	response, err := client.Get(url)
	if err != nil {
		return err
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, response.Status)
	}
	return nil
}

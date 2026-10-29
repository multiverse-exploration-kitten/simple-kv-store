package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLiveURLDialsLoopbackForAWildcardListenAddress(t *testing.T) {
	for httpAddress, want := range map[string]string{
		":8000":         "http://127.0.0.1:8000/health/live",
		"0.0.0.0:8000":  "http://127.0.0.1:8000/health/live",
		"[::]:8000":     "http://127.0.0.1:8000/health/live",
		"10.1.2.3:8080": "http://10.1.2.3:8080/health/live",
		"[::1]:8000":    "http://[::1]:8000/health/live",
	} {
		got, err := liveURL(httpAddress)
		if err != nil || got != want {
			t.Errorf("liveURL(%q) = %q, %v; want %q", httpAddress, got, err, want)
		}
	}
	if got, err := liveURL("8000"); err == nil {
		t.Errorf("liveURL(%q) = %q, want an error", "8000", got)
	}
}

func TestCheckLiveNeedsStatus200(t *testing.T) {
	client := &http.Client{Timeout: 2 * time.Second}
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer healthy.Close()
	if err := checkLive(client, healthy.URL); err != nil {
		t.Errorf("checkLive on a 200 server: %v", err)
	}

	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unhealthy.Close()
	if err := checkLive(client, unhealthy.URL); err == nil {
		t.Error("checkLive on a 503 server: want an error")
	}

	stopped := httptest.NewServer(http.NotFoundHandler())
	stopped.Close()
	if err := checkLive(client, stopped.URL); err == nil {
		t.Error("checkLive on a stopped server: want an error")
	}
}

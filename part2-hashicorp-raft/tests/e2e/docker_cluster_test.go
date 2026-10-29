//go:build e2e

// Package e2e drives the Docker Compose cluster: the docker CLI injects the
// faults, and net/http talks to the API on the host ports.
package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Own project name, host ports and subnet, so the e2e cluster can run next
// to `make up` (kvstore-part2, 8101-8103, 172.28.11.0/24).
const (
	projectName  = "kvstore-part2-e2e"
	subnetPrefix = "172.28.12"
)

var (
	services  = []string{"node1", "node2", "node3"}
	httpPorts = map[string]int{"node1": 18101, "node2": 18102, "node3": 18103}
	// docker-compose.yml gives nodeN the static IP <subnet prefix>.1N.
	staticHostNumbers = map[string]int{"node1": 11, "node2": 12, "node3": 13}
)

const (
	// Docker runs the production Raft timings (heartbeat and election 1 s).
	waitTimeout  = 60 * time.Second
	pollInterval = 200 * time.Millisecond
	// Longer than the worst API answer: the hold for a leader (5 s), then
	// one forward (2 x KV_COMMAND_TIMEOUT = 10 s).
	requestTimeout = 30 * time.Second
	statusTimeout  = 2 * time.Second
	logLinesOnFail = 80
)

type dockerCluster struct {
	partDir     string
	composeFile string
	environment []string
	client      *http.Client
}

// startDockerCluster builds and starts the cluster and waits for a leader.
// t.Cleanup deletes the containers, the network and the volumes, also when
// the test fails.
func startDockerCluster(t *testing.T) *dockerCluster {
	t.Helper()
	partDir, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("find the part directory: %v", err)
	}
	environment := append(os.Environ(), "KV_SUBNET_PREFIX="+subnetPrefix)
	for service, port := range httpPorts {
		environment = append(environment, fmt.Sprintf("KV_%s_HTTP_PORT=%d", strings.ToUpper(service), port))
	}
	cluster := &dockerCluster{
		partDir:     partDir,
		composeFile: filepath.Join(partDir, "docker-compose.yml"),
		environment: environment,
		// No keep-alive: a pooled connection to a killed node would fail.
		client: &http.Client{Timeout: requestTimeout, Transport: &http.Transport{DisableKeepAlives: true}},
	}
	// Remove what an interrupted earlier run left behind.
	cluster.compose(t, "down", "--volumes", "--remove-orphans")
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := cluster.run("docker", cluster.composeArguments("logs", "--no-color", "--tail", fmt.Sprint(logLinesOnFail))...)
			t.Logf("last %d log lines of each container:\n%s", logLinesOnFail, logs)
		}
		if output, err := cluster.run("docker", cluster.composeArguments("down", "--volumes", "--remove-orphans")...); err != nil {
			t.Errorf("docker compose down: %v\n%s", err, output)
		}
	})
	cluster.compose(t, "up", "--detach", "--build", "--wait")
	cluster.waitForLeader(t, services...)
	return cluster
}

// --- docker ---------------------------------------------------------------

func (cluster *dockerCluster) composeArguments(arguments ...string) []string {
	return append([]string{"compose", "--file", cluster.composeFile, "--project-name", projectName}, arguments...)
}

func (cluster *dockerCluster) compose(t *testing.T, arguments ...string) string {
	t.Helper()
	return cluster.mustRun(t, "docker", cluster.composeArguments(arguments...)...)
}

func (cluster *dockerCluster) run(name string, arguments ...string) (string, error) {
	command := exec.Command(name, arguments...)
	command.Dir = cluster.partDir
	command.Env = cluster.environment
	output, err := command.CombinedOutput()
	return string(output), err
}

func (cluster *dockerCluster) mustRun(t *testing.T, name string, arguments ...string) string {
	t.Helper()
	output, err := cluster.run(name, arguments...)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(arguments, " "), err, output)
	}
	return output
}

func (cluster *dockerCluster) networkName() string { return projectName + "_raft" }

func (cluster *dockerCluster) containerID(t *testing.T, service string) string {
	t.Helper()
	return strings.TrimSpace(cluster.compose(t, "ps", "--all", "--quiet", service))
}

// kill sends SIGKILL: no leadership transfer, no clean raft shutdown.
func (cluster *dockerCluster) kill(t *testing.T, service string) {
	t.Helper()
	cluster.compose(t, "kill", "--signal", "SIGKILL", service)
}

// stop sends SIGTERM and waits until the containers exited.
func (cluster *dockerCluster) stop(t *testing.T, stoppedServices ...string) {
	t.Helper()
	cluster.compose(t, append([]string{"stop"}, stoppedServices...)...)
}

func (cluster *dockerCluster) start(t *testing.T, startedServices ...string) {
	t.Helper()
	cluster.compose(t, append([]string{"start"}, startedServices...)...)
}

func (cluster *dockerCluster) pause(t *testing.T, service string) {
	t.Helper()
	cluster.compose(t, "pause", service)
}

func (cluster *dockerCluster) unpause(t *testing.T, service string) {
	t.Helper()
	cluster.compose(t, "unpause", service)
}

// disconnect removes the container from the Raft network. Its host port
// stops working too, because the port mapping uses that network.
func (cluster *dockerCluster) disconnect(t *testing.T, service string) {
	t.Helper()
	cluster.mustRun(t, "docker", "network", "disconnect", cluster.networkName(), cluster.containerID(t, service))
}

// connect adds the container to the Raft network again with its static IP,
// which is its Raft address.
func (cluster *dockerCluster) connect(t *testing.T, service string) {
	t.Helper()
	staticIP := fmt.Sprintf("%s.%d", subnetPrefix, staticHostNumbers[service])
	cluster.mustRun(t, "docker", "network", "connect", "--ip", staticIP, cluster.networkName(), cluster.containerID(t, service))
}

// containerState is "running", "paused", "exited", ...
func (cluster *dockerCluster) containerState(t *testing.T, service string) string {
	t.Helper()
	return strings.TrimSpace(cluster.mustRun(t, "docker", "inspect", "--format", "{{.State.Status}}", cluster.containerID(t, service)))
}

func (cluster *dockerCluster) isConnected(t *testing.T, service string) bool {
	t.Helper()
	template := fmt.Sprintf(`{{if index .NetworkSettings.Networks %q}}yes{{end}}`, cluster.networkName())
	return strings.TrimSpace(cluster.mustRun(t, "docker", "inspect", "--format", template, cluster.containerID(t, service))) == "yes"
}

// restore undoes every fault: connect, unpause and start each node. Then it
// waits for one leader that all nodes agree on.
func (cluster *dockerCluster) restore(t *testing.T) {
	t.Helper()
	for _, service := range services {
		if !cluster.isConnected(t, service) {
			cluster.connect(t, service)
		}
		switch cluster.containerState(t, service) {
		case "running":
		case "paused":
			cluster.unpause(t, service)
		default:
			cluster.start(t, service)
		}
	}
	cluster.waitForLeader(t, services...)
}

// --- HTTP -----------------------------------------------------------------

type apiResponse struct {
	status int
	header http.Header
	body   []byte
}

func (response apiResponse) String() string {
	return fmt.Sprintf("%d %s", response.status, strings.TrimSpace(string(response.body)))
}

func (cluster *dockerCluster) url(service, path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", httpPorts[service], path)
}

func (cluster *dockerCluster) do(method, service, path, body string) (apiResponse, error) {
	request, err := http.NewRequest(method, cluster.url(service, path), strings.NewReader(body))
	if err != nil {
		return apiResponse{}, err
	}
	response, err := cluster.client.Do(request)
	if err != nil {
		return apiResponse{}, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return apiResponse{}, err
	}
	return apiResponse{status: response.StatusCode, header: response.Header, body: responseBody}, nil
}

func (cluster *dockerCluster) mustDo(t *testing.T, method, service, path, body string) apiResponse {
	t.Helper()
	response, err := cluster.do(method, service, path, body)
	if err != nil {
		t.Fatalf("%s %s on %s: %v", method, path, service, err)
	}
	return response
}

func (cluster *dockerCluster) put(t *testing.T, service, key, value string) apiResponse {
	t.Helper()
	body, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		t.Fatalf("encode the value: %v", err)
	}
	return cluster.mustDo(t, http.MethodPut, service, "/kv/"+key, string(body))
}

func (cluster *dockerCluster) mustPut(t *testing.T, service, key, value string) {
	t.Helper()
	if response := cluster.put(t, service, key, value); response.status != http.StatusOK {
		t.Fatalf("PUT %s=%s on %s = %s, want 200", key, value, service, response)
	}
}

func (cluster *dockerCluster) get(t *testing.T, service, key, consistency string) apiResponse {
	t.Helper()
	return cluster.mustDo(t, http.MethodGet, service, "/kv/"+key+"?consistency="+consistency, "")
}

// value returns the value of a 200 answer.
func value(t *testing.T, response apiResponse) (string, bool) {
	t.Helper()
	if response.status != http.StatusOK {
		return "", false
	}
	var entry struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(response.body, &entry); err != nil {
		t.Fatalf("decode %s: %v", response, err)
	}
	return entry.Value, true
}

func requireValue(t *testing.T, response apiResponse, want string) {
	t.Helper()
	if got, found := value(t, response); !found || got != want {
		t.Fatalf("read = %s, want 200 with value %q", response, want)
	}
}

type nodeStatus struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
	Ready    bool   `json:"ready"`
}

func (cluster *dockerCluster) status(service string) (nodeStatus, error) {
	request, err := http.NewRequest(http.MethodGet, cluster.url(service, "/status"), nil)
	if err != nil {
		return nodeStatus{}, err
	}
	client := &http.Client{Timeout: statusTimeout, Transport: cluster.client.Transport}
	response, err := client.Do(request)
	if err != nil {
		return nodeStatus{}, err
	}
	defer response.Body.Close()
	var status nodeStatus
	return status, json.NewDecoder(response.Body).Decode(&status)
}

// --- waits ----------------------------------------------------------------

// waitUntil polls probe until it returns true. A failed wait names the
// condition and the last thing the probe saw.
func waitUntil(t *testing.T, description string, probe func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	lastSeen := "nothing yet"
	for time.Now().Before(deadline) {
		done, seen := probe()
		if done {
			return
		}
		lastSeen = seen
		time.Sleep(pollInterval)
	}
	t.Fatalf("timed out after %s waiting for %s. Last seen:\n%s", waitTimeout, description, lastSeen)
}

// waitForLeader waits until the nodes in among agree on one leader and one
// term, the leader is one of them and is in the leader state, and every one
// of them is ready (a follower heard from the leader recently, the leader
// finished its barrier). It returns the leader and the term.
func (cluster *dockerCluster) waitForLeader(t *testing.T, among ...string) (leader string, term uint64) {
	t.Helper()
	waitUntil(t, "one ready leader that "+strings.Join(among, ", ")+" agree on", func() (bool, string) {
		statuses := map[string]nodeStatus{}
		var seen strings.Builder
		for _, service := range among {
			status, err := cluster.status(service)
			if err != nil {
				fmt.Fprintf(&seen, "%s: unreachable (%v)\n", service, err)
				continue
			}
			statuses[service] = status
			fmt.Fprintf(&seen, "%s: state=%s term=%d leader=%q ready=%t\n", service, status.State, status.Term, status.LeaderID, status.Ready)
		}
		if len(statuses) != len(among) {
			return false, seen.String()
		}
		first := statuses[among[0]]
		for _, status := range statuses {
			if status.LeaderID == "" || status.LeaderID != first.LeaderID || status.Term != first.Term || !status.Ready {
				return false, seen.String()
			}
		}
		leaderStatus, isMember := statuses[first.LeaderID]
		if !isMember || leaderStatus.State != "Leader" {
			return false, seen.String()
		}
		leader, term = first.LeaderID, first.Term
		return true, ""
	})
	return leader, term
}

// waitForValue waits until a stale (local) read on service returns value.
func (cluster *dockerCluster) waitForValue(t *testing.T, service, key, want string) {
	t.Helper()
	waitUntil(t, fmt.Sprintf("%s to return %s=%q in a stale read", service, key, want), func() (bool, string) {
		response, err := cluster.do(http.MethodGet, service, "/kv/"+key+"?consistency=stale", "")
		if err != nil {
			return false, err.Error()
		}
		got, found := value(t, response)
		return found && got == want, response.String()
	})
}

func others(all []string, excluded ...string) []string {
	var remaining []string
	for _, service := range all {
		if !slices.Contains(excluded, service) {
			remaining = append(remaining, service)
		}
	}
	return remaining
}

// keyFor returns a key that no other test uses.
func keyFor(t *testing.T) string {
	return "e2e-" + strings.ReplaceAll(t.Name(), "/", "-")
}

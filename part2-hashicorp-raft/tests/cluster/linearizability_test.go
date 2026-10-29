package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const (
	// linearizabilitySeedVariable replays a seed of the fault test. The seed
	// fixes the operations of each client and the fault schedule. It does
	// not fix the thread timing, so a replay is close, not exact.
	linearizabilitySeedVariable = "LINEARIZABILITY_SEED"

	linearizabilityWorkload    = 12 * time.Second
	linearizabilityClientCount = 5
	// linearizabilityRejectedBackoff: a client waits a little after a
	// rejection (HTTP 503 says Retry-After), so it does not spin while the
	// cluster has no leader.
	linearizabilityRejectedBackoff   = 20 * time.Millisecond
	linearizabilityMinimumOperations = 20
	linearizabilityNemesisStream     = 1000 // PCG stream of the nemesis; clients use 0..N-1
)

var linearizabilityKeys = []string{"key-a", "key-b", "key-c"}

// Scenario: 5 clients run put and linearizable get on 3 keys through random
// nodes for 12 s. A client that gets not_leader follows it to the leader
// once. On the test goroutine a nemesis repeats: a healthy pause, then one
// fault (isolate the leader or a node, crash the leader or a node, restart a
// node, or cut every node from the others), then heal and restart. After the
// faults one linearizable read per key runs on the healed cluster.
// Expected: Porcupine finds a linearization of the whole history. Writes
// that ended with "outcome unknown" stay open, rejected operations are
// dropped. At least 20 puts and 20 gets complete.
// Raft lesson: a Raft store is linearizable only if every layer is right at
// once: reads wait for the leader's barrier and VerifyLeader, a timeout is
// "maybe" and not "failed", and a rejection really means "not applied". The
// check tests the error contract too: a rejected write that took effect
// shows up as a read of a value that no operation in the history wrote.
// Observed: when the recorder drops "outcome unknown" writes as failed,
// Porcupine reports a violation in every run. Some of those writes always
// committed, and a later read returned them.
func TestLinearizabilityPutAndGetUnderFaults(t *testing.T) {
	seed := linearizabilitySeed(t)
	t.Logf("seed %d: replay with %s=%d", seed, linearizabilitySeedVariable, seed)
	cluster := New(t, Options{})
	cluster.WaitForLeader()
	recorder := newHistoryRecorder()

	stop := make(chan struct{})
	var clients sync.WaitGroup
	clientErrors := make([]error, linearizabilityClientCount)
	for client := range linearizabilityClientCount {
		random := rand.New(rand.NewPCG(uint64(seed), uint64(client)))
		clients.Go(func() {
			clientErrors[client] = linearizabilityRunClient(cluster, recorder, client, random, stop)
		})
	}
	var stopOnce sync.Once
	stopClients := func() {
		stopOnce.Do(func() { close(stop) })
		clients.Wait()
	}
	// Cleanups run last in, first out: the clients stop before the cluster.
	t.Cleanup(stopClients)

	nemesis := &linearizabilityNemesis{
		t:        t,
		cluster:  cluster,
		recorder: recorder,
		random:   rand.New(rand.NewPCG(uint64(seed), linearizabilityNemesisStream)),
	}
	nemesis.run(linearizabilityWorkload)
	stopClients()
	if err := errors.Join(clientErrors...); err != nil {
		t.Fatal(err)
	}

	nemesis.repair()
	const finalClient = linearizabilityClientCount
	for _, key := range linearizabilityKeys {
		input := historyInput{Kind: historyGet, Key: key, Consistency: node.Linearizable}
		WaitUntil(t, 10*time.Second, "a linearizable read of "+key+" after the heal", func() bool {
			return linearizabilityExecute(cluster, recorder, finalClient, cluster.WaitForLeader(), input) == nil
		})
	}

	counts := recorder.counts()
	t.Logf("history: %+v, %d faults", counts, nemesis.faultCount)
	if counts.CompletedPuts < linearizabilityMinimumOperations || counts.CompletedGets < linearizabilityMinimumOperations {
		t.Fatalf("only %d puts and %d gets completed, want at least %d of each: the history proves too little",
			counts.CompletedPuts, counts.CompletedGets, linearizabilityMinimumOperations)
	}
	if result := historyCheck(t, recorder); result != porcupine.Ok {
		t.Fatalf("porcupine result = %s, want %s. Replay with %s=%d", result, porcupine.Ok, linearizabilitySeedVariable, seed)
	}
}

// Scenario: write v1 and wait until every node applied it. Cut one follower
// off. Write v2 through the leader: it commits with 2 of 3 nodes. Then read
// the key with consistency=stale on the cut follower.
// Expected: the stale read returns v1 after the write of v2 completed.
// Porcupine accepts the history before that read and reports the history
// with the read as not linearizable.
// Raft lesson: a stale read (and a leader read on an old leader that did
// not step down yet) is a local read without a quorum check, so it can go
// back in time. A checker is only worth something if it can fail: this is a
// real stale read, and Porcupine finds it.
//
// A leader read on an isolated old leader is the same bug, but only in the
// short time before the old leader steps down. raft does not allow a
// LeaderLeaseTimeout above the HeartbeatTimeout, so a new leader cannot
// reliably finish a write in that time. The stale read on a cut follower is
// deterministic. The timeouts are 500 ms: a 230 ms process pause under
// parallel -race load must not end the lease of the leader that has only one
// follower.
func TestLinearizabilityCheckerFindsStaleRead(t *testing.T) {
	cluster := New(t, Options{Timing: node.Timing{
		HeartbeatTimeout:   500 * time.Millisecond,
		ElectionTimeout:    500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      10 * time.Millisecond,
	}})
	leader := cluster.WaitForLeader()
	cutFollower := cluster.Followers(leader.ID())[0]
	recorder := newHistoryRecorder()
	const key, writer, reader = "balance", 0, 1

	linearizabilityMustRun(t, recorder, writer, leader, historyInput{Kind: historyPut, Key: key, Value: "v1"})
	cluster.WaitForValueEverywhere(key, "v1")
	cluster.Isolate(cutFollower.ID())
	linearizabilityMustRun(t, recorder, writer, leader, historyInput{Kind: historyPut, Key: key, Value: "v2"})
	if result := historyCheck(t, recorder); result != porcupine.Ok {
		t.Fatalf("porcupine result before the stale read = %s, want %s", result, porcupine.Ok)
	}

	read := linearizabilityMustRun(t, recorder, reader, cutFollower, historyInput{Kind: historyGet, Key: key, Consistency: node.Stale})
	if !read.Found || read.Value != "v1" {
		t.Fatalf("stale read on the cut follower %s = %+v, want the old value v1", cutFollower.ID(), read)
	}
	if result := historyCheck(t, recorder); result != porcupine.Illegal {
		t.Fatalf("porcupine result with the stale read = %s, want %s: the checker missed a stale read", result, porcupine.Illegal)
	}
}

func linearizabilitySeed(t *testing.T) int64 {
	text := os.Getenv(linearizabilitySeedVariable)
	if text == "" {
		return time.Now().UnixNano()
	}
	seed, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		t.Fatalf("%s=%q is not an int64: %v", linearizabilitySeedVariable, text, err)
	}
	return seed
}

// linearizabilityRunClient runs random operations until stop is closed. Each
// put writes a value that no other put writes. It returns an error only for
// an error that is neither a rejection nor "outcome unknown".
func linearizabilityRunClient(cluster *Cluster, recorder *historyRecorder, client int, random *rand.Rand, stop <-chan struct{}) error {
	nodeIDs := cluster.IDs()
	for sequence := 1; ; sequence++ {
		select {
		case <-stop:
			return nil
		default:
		}
		// Draw every choice first, so the random stream does not depend on timing.
		targetID := nodeIDs[random.IntN(len(nodeIDs))]
		input := historyInput{Kind: historyGet, Key: linearizabilityKeys[random.IntN(len(linearizabilityKeys))], Consistency: node.Linearizable}
		if random.IntN(2) == 0 {
			input = historyInput{Kind: historyPut, Key: input.Key, Value: fmt.Sprintf("c%d-%d", client, sequence)}
		}
		target := cluster.Node(targetID)
		if target == nil {
			continue // the nemesis stopped it
		}

		err := linearizabilityExecute(cluster, recorder, client, target, input)
		var rejected *node.RejectedError
		var unknown *node.OutcomeUnknownError
		switch {
		case errors.As(err, &rejected):
			time.Sleep(linearizabilityRejectedBackoff)
		case err != nil && !errors.As(err, &unknown):
			return fmt.Errorf("client %d: %s %s on %s: unexpected error: %w", client, input.Kind, input.Key, target.ID(), err)
		}
	}
}

// linearizabilityExecute runs one operation and records it. Like the HTTP
// layer, it follows a not_leader answer to the leader once. The first try
// was rejected, so it had no effect: one operation covers both tries.
func linearizabilityExecute(cluster *Cluster, recorder *historyRecorder, client int, target *node.Node, input historyInput) error {
	call := recorder.begin(client, input)
	output, err := linearizabilityApply(target, input)
	servedBy := target.ID()
	var rejected *node.RejectedError
	if errors.As(err, &rejected) && rejected.Reason == node.ReasonNotLeader {
		if leader := cluster.Node(rejected.LeaderID); leader != nil {
			servedBy = fmt.Sprintf("%s (redirected by %s)", leader.ID(), target.ID())
			output, err = linearizabilityApply(leader, input)
		}
	}
	recorder.finish(call, output, servedBy, err)
	return err
}

// linearizabilityMustRun runs one operation on target, records it and fails
// the test on any error.
func linearizabilityMustRun(t *testing.T, recorder *historyRecorder, client int, target *node.Node, input historyInput) historyOutput {
	t.Helper()
	call := recorder.begin(client, input)
	output, err := linearizabilityApply(target, input)
	recorder.finish(call, output, target.ID(), err)
	if err != nil {
		t.Fatalf("%+v on %s: %v", input, target.ID(), err)
	}
	return output
}

func linearizabilityApply(target *node.Node, input historyInput) (historyOutput, error) {
	if input.Kind == historyPut {
		_, err := target.Put(context.Background(), input.Key, input.Value, node.WriteOptions{})
		return historyOutput{}, err
	}
	read, err := target.Get(context.Background(), input.Key, node.ReadOptions{Consistency: input.Consistency})
	return historyOutput{Found: read.Found, Value: read.Entry.Value}, err
}

// linearizabilityNemesis injects faults. It runs on the test goroutine,
// because the harness methods that fail the test must run there.
type linearizabilityNemesis struct {
	t          *testing.T
	cluster    *Cluster
	recorder   *historyRecorder
	random     *rand.Rand
	faultCount int
}

// run repeats a healthy pause, one fault and a repair until duration ends.
// Each fault is an annotation in the visualization.
func (nemesis *linearizabilityNemesis) run(duration time.Duration) {
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		nemesis.hold(200*time.Millisecond, 600*time.Millisecond)
		start := nemesis.recorder.now()
		description := nemesis.injectFault()
		nemesis.hold(300*time.Millisecond, 1200*time.Millisecond)
		nemesis.repair()
		nemesis.recorder.annotate(description, start, nemesis.recorder.now())
		nemesis.faultCount++
		nemesis.t.Logf("nemesis: %s", description)
	}
}

// hold keeps the current state for a random time. This sleep is the fault
// schedule, not a wait for the cluster.
func (nemesis *linearizabilityNemesis) hold(minimum, maximum time.Duration) {
	time.Sleep(minimum + time.Duration(nemesis.random.Int64N(int64(maximum-minimum))))
}

func (nemesis *linearizabilityNemesis) injectFault() string {
	nodeIDs := nemesis.cluster.IDs()
	victim := nodeIDs[nemesis.random.IntN(len(nodeIDs))]
	fault := nemesis.random.IntN(6)
	leader, leaderLabel := victim, "node "+victim+" (no leader known)"
	for _, running := range nemesis.cluster.Nodes() {
		if running.IsLeader() {
			leader, leaderLabel = running.ID(), "leader "+running.ID()
		}
	}
	switch fault {
	case 0:
		nemesis.cluster.Isolate(leader)
		return "isolate " + leaderLabel
	case 1:
		nemesis.cluster.Isolate(victim)
		return "isolate " + victim
	case 2:
		nemesis.cluster.Stop(leader)
		return "crash " + leaderLabel
	case 3:
		nemesis.cluster.Stop(victim)
		return "crash " + victim
	case 4:
		nemesis.cluster.Restart(victim)
		return "restart " + victim
	default:
		for _, id := range nodeIDs {
			nemesis.cluster.Isolate(id)
		}
		return "cut every node from the others (no quorum)"
	}
}

// repair heals the network and starts every stopped node.
func (nemesis *linearizabilityNemesis) repair() {
	nemesis.cluster.Heal()
	for _, id := range nemesis.cluster.IDs() {
		if nemesis.cluster.Node(id) == nil {
			nemesis.cluster.Start(id)
		}
	}
}

// Scenario: a scripted history on one key, without a cluster. put v1
// completes, put v2 ends with "outcome unknown", put v3 is rejected. Then
// one client reads the key one or more times.
// Expected: a read may return v1 or v2, because the open write may or may
// not have taken effect. v2 and then v1 is not linearizable: the open write
// took effect at one point or never. v3 is not linearizable: a rejected
// write did not happen.
// Raft lesson: a write that timed out is not a failed write. The history
// must keep it as "maybe" and drop only the writes that are known not
// applied (RejectedError, HTTP 503).
func TestLinearizabilityModelOpenAndRejectedWrites(t *testing.T) {
	tests := []struct {
		name  string
		reads []string
		want  porcupine.CheckResult
	}{
		{name: "open write did not take effect", reads: []string{"v1"}, want: porcupine.Ok},
		{name: "open write took effect", reads: []string{"v2", "v2"}, want: porcupine.Ok},
		{name: "open write took effect and then went away", reads: []string{"v2", "v1"}, want: porcupine.Illegal},
		{name: "rejected write took effect", reads: []string{"v3"}, want: porcupine.Illegal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const key, writer, reader = "account", 0, 1
			recorder := newHistoryRecorder()
			for _, write := range []struct {
				value string
				err   error
			}{
				{value: "v1", err: nil},
				{value: "v2", err: &node.OutcomeUnknownError{Reason: node.ReasonTimeout}},
				{value: "v3", err: &node.RejectedError{Reason: node.ReasonNotLeader}},
			} {
				call := recorder.begin(writer, historyInput{Kind: historyPut, Key: key, Value: write.value})
				recorder.finish(call, historyOutput{}, "node1", write.err)
			}
			for _, value := range test.reads {
				call := recorder.begin(reader, historyInput{Kind: historyGet, Key: key, Consistency: node.Linearizable})
				recorder.finish(call, historyOutput{Found: true, Value: value}, "node1", nil)
			}

			operations, _ := recorder.history()
			if got := porcupine.CheckOperationsTimeout(historyModel, operations, historyCheckTimeout); got != test.want {
				t.Fatalf("porcupine result = %s, want %s for reads %v", got, test.want, test.reads)
			}
			if counts := recorder.counts(); counts.Open != 1 || counts.Dropped != 1 || counts.Processes != 3 {
				t.Fatalf("counts = %+v, want 1 open, 1 dropped, 3 processes (the writer gets a new process after its open write)", counts)
			}
		})
	}
}

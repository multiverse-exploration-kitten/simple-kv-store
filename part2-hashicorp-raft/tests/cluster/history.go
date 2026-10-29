package cluster

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/multiverse-exploration-kitten/simple-kv-store/part2-hashicorp-raft/internal/node"
)

const (
	// historyVisualizationDirVariable names a directory that keeps the
	// Porcupine visualization. Without it the file goes to t.TempDir(),
	// which Go deletes when the test ends.
	historyVisualizationDirVariable = "LINEARIZABILITY_VISUALIZATION_DIR"

	historyCheckTimeout = time.Minute

	// historyNeverReturned is the return time of an open operation. It is
	// after every other operation, so Porcupine can put the operation at any
	// point after its call, also after the last read (= it never took effect).
	historyNeverReturned int64 = math.MaxInt64

	historyNemesisTag = "nemesis"
)

type historyOperationKind string

const (
	historyPut historyOperationKind = "put"
	historyGet historyOperationKind = "get"
)

// historyInput is what a client asked for.
type historyInput struct {
	Kind        historyOperationKind
	Key         string
	Value       string               // put
	Consistency node.ReadConsistency // get
}

// historyOutput is what the client saw.
type historyOutput struct {
	Found   bool   // get
	Value   string // get
	Unknown bool   // the client never learned the outcome
}

// historyRegister is the model state of one key.
type historyRegister struct {
	Found bool
	Value string
}

// historyCall is an operation that started and did not end yet.
type historyCall struct {
	client   int
	process  int
	input    historyInput
	callTime int64
}

// historyCounts summarizes a history.
type historyCounts struct {
	CompletedPuts int
	CompletedGets int
	Open          int
	Dropped       int
	Processes     int
}

// historyRecorder records client operations for Porcupine. It is safe for
// concurrent use.
//
// Like Jepsen, it gives a client a new process (Porcupine ClientId, one row
// in the visualization) after an open operation: the old process still
// waits for its answer.
type historyRecorder struct {
	start time.Time

	mutex           sync.Mutex
	lastTime        int64
	operations      []porcupine.Operation
	annotations     []porcupine.Annotation
	processOfClient map[int]int
	nextProcess     int
	dropped         int
}

func newHistoryRecorder() *historyRecorder {
	return &historyRecorder{start: time.Now(), processOfClient: map[int]int{}}
}

// timeLocked returns nanoseconds since the start. Each value is larger than
// the one before: the clock ticks in about 42 ns on Apple silicon, and a tie
// makes two sequential operations look concurrent. The caller holds the mutex.
func (recorder *historyRecorder) timeLocked() int64 {
	recorder.lastTime = max(recorder.lastTime+1, time.Since(recorder.start).Nanoseconds())
	return recorder.lastTime
}

// now returns the recorder time, for annotations.
func (recorder *historyRecorder) now() int64 {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.timeLocked()
}

// begin takes the call time. Call it just before the request.
func (recorder *historyRecorder) begin(client int, input historyInput) historyCall {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	process, exists := recorder.processOfClient[client]
	if !exists {
		process = recorder.nextProcess
		recorder.nextProcess++
		recorder.processOfClient[client] = process
	}
	return historyCall{client: client, process: process, input: input, callTime: recorder.timeLocked()}
}

// finish takes the return time and adds the operation. err is the error that
// the client saw:
//   - nil: the operation completed with output.
//   - *node.RejectedError: the operation did not happen. It is dropped.
//   - any other error, such as *node.OutcomeUnknownError: the operation stays
//     open. It may take effect at any time after its call, or never. Only a
//     RejectedError proves that nothing happened.
//
// Porcupine 1.3.1 has no pending operations. An operation with a return time
// after all others and an output that the model accepts in every state is
// the same thing (Porcupine's own Jepsen test does this).
func (recorder *historyRecorder) finish(call historyCall, output historyOutput, servedBy string, err error) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	returnTime := recorder.timeLocked()
	var rejected *node.RejectedError
	if errors.As(err, &rejected) {
		recorder.dropped++
		return
	}
	metadata := "served by " + servedBy
	if err != nil {
		output = historyOutput{Unknown: true}
		returnTime = historyNeverReturned
		metadata += ": " + err.Error()
		delete(recorder.processOfClient, call.client)
	}
	recorder.operations = append(recorder.operations, porcupine.Operation{
		ClientId: call.process,
		Input:    call.input,
		Call:     call.callTime,
		Output:   output,
		Return:   returnTime,
		Metadata: metadata,
	})
}

// annotate marks a fault in the visualization.
func (recorder *historyRecorder) annotate(description string, start, end int64) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.annotations = append(recorder.annotations, porcupine.Annotation{
		Tag:         historyNemesisTag,
		Start:       start,
		End:         end,
		Description: description,
	})
}

// history returns copies of the operations and the annotations.
func (recorder *historyRecorder) history() ([]porcupine.Operation, []porcupine.Annotation) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return slices.Clone(recorder.operations), slices.Clone(recorder.annotations)
}

func (recorder *historyRecorder) counts() historyCounts {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	counts := historyCounts{Dropped: recorder.dropped, Processes: recorder.nextProcess}
	for _, operation := range recorder.operations {
		switch {
		case operation.Return == historyNeverReturned:
			counts.Open++
		case operation.Input.(historyInput).Kind == historyPut:
			counts.CompletedPuts++
		default:
			counts.CompletedGets++
		}
	}
	return counts
}

// historyModel is a key-value store of registers. Porcupine checks each key
// alone (P-compositionality), which keeps the check fast.
var historyModel = porcupine.Model{
	Partition: historyPartitionByKey,
	Init:      func() any { return historyRegister{} },
	Step:      historyStep,
	DescribeOperation: func(input, output any) string {
		return historyDescribeOperation(input.(historyInput), output.(historyOutput))
	},
	DescribeState: func(state any) string { return historyDescribeRegister(state.(historyRegister)) },
	DescribeOperationMetadata: func(metadata any) string {
		text, _ := metadata.(string)
		return text
	},
}

func historyPartitionByKey(operations []porcupine.Operation) [][]porcupine.Operation {
	byKey := map[string][]porcupine.Operation{}
	for _, operation := range operations {
		key := operation.Input.(historyInput).Key
		byKey[key] = append(byKey[key], operation)
	}
	var partitions [][]porcupine.Operation
	for _, key := range slices.Sorted(maps.Keys(byKey)) {
		partitions = append(partitions, byKey[key])
	}
	return partitions
}

func historyStep(state, input, output any) (bool, any) {
	register := state.(historyRegister)
	operation := input.(historyInput)
	result := output.(historyOutput)
	if operation.Kind == historyPut {
		return true, historyRegister{Found: true, Value: operation.Value}
	}
	observed := historyRegister{Found: result.Found, Value: result.Value}
	return result.Unknown || observed == register, register
}

func historyDescribeOperation(operation historyInput, result historyOutput) string {
	switch {
	case operation.Kind == historyPut && result.Unknown:
		return fmt.Sprintf("put(%s, %s) -> unknown", operation.Key, operation.Value)
	case operation.Kind == historyPut:
		return fmt.Sprintf("put(%s, %s)", operation.Key, operation.Value)
	case result.Unknown:
		return fmt.Sprintf("get(%s, %s) -> unknown", operation.Key, operation.Consistency)
	default:
		return fmt.Sprintf("get(%s, %s) -> %s", operation.Key, operation.Consistency,
			historyDescribeRegister(historyRegister{Found: result.Found, Value: result.Value}))
	}
}

func historyDescribeRegister(register historyRegister) string {
	if !register.Found {
		return "missing"
	}
	return register.Value
}

// historyCheck checks the recorded history with Porcupine. When the result
// is not Ok, it writes the visualization (with the nemesis faults) and logs
// its path.
func historyCheck(t *testing.T, recorder *historyRecorder) porcupine.CheckResult {
	t.Helper()
	operations, annotations := recorder.history()
	started := time.Now()
	result, info := porcupine.CheckOperationsVerbose(historyModel, operations, historyCheckTimeout)
	t.Logf("porcupine: %s for %d operations in %s", result, len(operations), time.Since(started).Round(time.Millisecond))
	if result == porcupine.Ok {
		return result
	}
	info.AddAnnotations(annotations)
	path := filepath.Join(historyVisualizationDir(t), strings.ReplaceAll(t.Name(), "/", "_")+".html")
	if err := porcupine.VisualizePath(historyModel, info, path); err != nil {
		t.Errorf("write the porcupine visualization: %v", err)
		return result
	}
	t.Logf("porcupine visualization: %s", path)
	return result
}

func historyVisualizationDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv(historyVisualizationDirVariable)
	if dir == "" {
		t.Logf("set %s to keep the visualization: Go deletes t.TempDir() when the test ends", historyVisualizationDirVariable)
		return t.TempDir()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create %s=%s: %v", historyVisualizationDirVariable, dir, err)
	}
	return dir
}

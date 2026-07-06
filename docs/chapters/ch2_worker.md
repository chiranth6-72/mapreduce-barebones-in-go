# Chapter 2: The Worker — Task Execution, Intermediate Partitioning & RPC Loop

## 1. Architectural Overview

The Worker is the compute muscle of the system. It registers with the Coordinator, then enters an infinite polling loop: request a task → execute it → report the result → repeat. Workers are stateless and interchangeable — any worker can execute any map or reduce task because all data lives on the shared filesystem.

**Lifecycle:**

```
  Boot ──→ RegisterWithCoordinator (retry loop)
              │
              ▼
         SendHeartbeat (1s goroutine)
              │
              ▼
    ┌─────────────────────┐
    │   Main Poll Loop     │
    │  ┌─────────────────┐ │
    │  │ RequestTask()   │ │ ← ask Coordinator for work
    │  └────────┬────────┘ │
    │           ▼          │
    │  ┌─────────────────┐ │
    │  │  ExecuteTask()   │ │ ← either ExecuteMapTask or ExecuteReduceTask
    │  └────────┬────────┘ │
    │           ▼          │
    │  ┌─────────────────┐ │
    │  │ ReportTask()     │ │ ← COMPLETED or FAILED
    │  └─────────────────┘ │
    └─────────────────────┘
```

**Key design decisions:**
- Workers **pull** tasks (push would require the Coordinator to know worker availability up front)
- Worker identity is a simple string ID; no authentication or encryption
- Intermediate data is key-hashed to reduce-bucket files on the shared filesystem (the "shuffle" is implicit in the file naming convention `mr-{mapIdx}-{reduceIdx}`)

## 2. Component Design & Structural Layout

### Key Abstractions

| Structure | Role |
|---|---|
| `Worker` | Holds ID, coordinator address, storage path, active flag; manages RPC connections |
| `Mapper` interface | `Map(key, value string, output chan<- KeyValue)` — transforms input records to intermediate key-value pairs |
| `Reducer` interface | `Reduce(key string, values <-chan string, output chan<- string)` — merges values for a key into a single result |
| `Task` (from shared) | Describes what work to do: type (MAP/REDUCE), index, job affiliation |

### Data Format — Intermediate File Layout

```
Intermediate storage layout (shared-hdfs/output/intermediate/):

  mr-{mapTaskIndex}-{reduceBucketIndex}

Example with 4 map tasks and 2 reduce buckets:

  mr-0-0    mr-0-1
  mr-1-0    mr-1-1
  mr-2-0    mr-2-1
  mr-3-0    mr-3-1

Each file is a flat TSV (tab-separated):
  key\tvalue\n
  key\tvalue\n
  ...
```

### Memory Layout — Map Task Execution Flow

```text
  Input File (e.g., doc-0.txt)
           │
           ▼
  mapper.Map(filename, content, outputChan)
           │
           │ emits KeyValue{Key, Value} per word/record
           ▼
  ┌─────────────────────────────┐
  │  Writer goroutine           │
  │  for kv := range outputChan │
  │    r := Ihash(kv.Key) % R   │  ← FNV-1a hash → reduce bucket
  │    fmt.Fprintf(files[r],    │
  │      "%s\t%s\n", kv.Key,    │
  │      kv.Value)              │
  └─────────────────────────────┘
           │
           ▼
  Files: mr-{mapIdx}-0, mr-{mapIdx}-1, ..., mr-{mapIdx}-{R-1}
```

## 3. Core Algorithms & Time/Space Complexities

### `ExecuteMapTask` — O(I + K), I = input size, K = emitted kv pairs

1. **Report** task as `TaskRunning` to Coordinator.
2. **Read input** — selects the file `inputFiles[task.TaskIndex % len(inputFiles)]` from the shared input directory.
3. **Create intermediate files** — opens R files (`mr-{taskIndex}-{r}` for r=0..R-1) in `output/intermediate/`.
4. **Start writer goroutine** — reads from a buffered `outputChan` (capacity 1000), hashes each key via `shared.Ihash()` to compute `r := hash(key) % R`, and writes `key\tvalue\n` to the corresponding file.
5. **Execute mapper** — calls `mapper.Map(filename, content, outputChan)` synchronously. The mapper pushes `KeyValue` pairs onto the channel.
6. **Wait** — closes `outputChan`, joins the writer goroutine, closes all intermediate files.
7. **Report** task as `TaskCompleted`.

### `ExecuteReduceTask` — O(F × L × log S), F = intermediate files to read, L = lines per file, S = unique keys

1. **Report** task as `TaskRunning`.
2. **Discover input files** — scans `output/intermediate/` for files matching `mr-*-{task.TaskIndex}` (e.g., `mr-*-2` for reduce bucket 2).
3. **Group by key** — reads each matching file line by line (`key\tvalue`), appending values to `map[string][]string`.
4. **Write output** — creates `output/final/part-{taskIndex}`.
5. **Reduce each key** — for each unique key:
   - Load all values into a buffered channel (`valueChan`).
   - Start a result writer goroutine that reads from `outputChan`.
   - Call `reducer.Reduce(key, valueChan, outputChan)`.
   - Close `outputChan`, wait for the writer to flush the result line.
6. **Report** task as `TaskCompleted`.

### `Ihash` (shared utility) — O(len(key))

Uses **FNV-1a 32-bit hash** (`hash/fnv.New32a()`), masked to a positive integer via `& 0x7fffffff`. Provides a uniform distribution of keys across reduce buckets for balanced reducer workloads.

### Heartbeat Routine — O(1) per tick

A goroutine sends a `Heartbeat` RPC to the Coordinator every second. If the Coordinator doesn't recognize the worker (e.g., after a Coordinator restart), the worker re-registers.

## 4. Code Architecture & Reference

```go
// worker/main.go — Worker struct
type Worker struct {
    mu          sync.Mutex
    id          string
    coordinator string
    storagePath string
    active      bool
}

// Main execution loop
func (w *Worker) Run() {
    // Retry registration until successful
    for {
        err := w.RegisterWithCoordinator()
        if err == nil { break }
        time.Sleep(2 * time.Second)
    }

    go w.SendHeartbeat() // 1-second ticker

    for w.active {
        taskResp, err := w.RequestTask()
        if err != nil || taskResp.Task == nil {
            time.Sleep(1 * time.Second)
            continue
        }

        if taskResp.Task.TaskType == shared.TaskMap {
            w.ExecuteMapTask(task, job, mapperCode, inputPath, outputPath)
        } else {
            w.ExecuteReduceTask(task, job, reducerCode, inputPath, outputPath)
        }
    }
}
```

**Key implementation detail — Intermediate partitioning in `ExecuteMapTask`:**
```go
outputChan := make(chan shared.KeyValue, 1000)
var wg sync.WaitGroup
wg.Add(1)
go func() {
    defer wg.Done()
    for kv := range outputChan {
        r := shared.Ihash(kv.Key) % job.NumReduceTasks
        fmt.Fprintf(files[r], "%s\t%s\n", kv.Key, kv.Value)
    }
}()
mapper.Map(inputFile.Name(), string(content), outputChan)
close(outputChan)
wg.Wait()
```

**Key implementation detail — Reduce-side grouping in `ExecuteReduceTask`:**
```go
keyValues := make(map[string][]string)
suffix := fmt.Sprintf("-%d", task.TaskIndex)
for _, file := range files {
    if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), suffix) {
        // Read file, split lines, parse key\tvalue, append to keyValues[key]
    }
}
// Then for each key, create value channel, spawn result writer, call reducer.Reduce
```

## 5. Trade-offs & Concurrency Considerations

### Thread Safety & Sync

- **Per-worker `sync.Mutex`** protects only the `active` flag. It is never held during task execution or RPC calls, so it introduces zero contention in the hot path.
- **Map task concurrency:** The mapper runs in the main goroutine while a dedicated goroutine drains `outputChan` and writes to files. Channel send/receive is synchronized by Go's runtime — no mutex needed for the `KeyValue` pipeline.
- **Reduce task concurrency:** Each key's reduction uses a pair of channels (`valueChan` → reducer → `outputChan` → writer goroutine). Keys are processed sequentially in the outer loop, so there is no concurrent map mutation.

### Engineering Bottlenecks

| Bottleneck | Impact |
|---|---|
| **Single-threaded reduce** | Keys are processed one at a time. For a skew-heavy dataset (one key with millions of values), reduce throughput is bounded by a single core. A production system would parallelize within a single reduce task or use combiners. |
| **In-memory key grouping** | `ExecuteReduceTask` loads all values for all keys in its partition into `map[string][]string`. For large partitions, this can exhaust heap. A production system would external-sort on disk. |
| **No streaming intermediate pipe** | The mapper writes to disk files, then the reducer reads them back. This is faithful to the original MapReduce paper's batch model, but sacrifices the latency improvements of pipelined shuffling (as in Hadoop's shuffle-on-the-fly). |
| **Gob-encoded code dispatch** | Mapper/reducer implementations are serialized with `encoding/gob` and resolved by string name (`getMapperByName`, `getReducerByName`). This means the worker binary must be compiled with the same types — a form of static plugin rather than dynamic sandboxed code loading. |
| **Stale file selection** | The map task picks `inputFiles[taskIndex % len(inputFiles)]`. If there are more map tasks than input files, multiple mappers process the same file (idempotent but wasteful). A production system would split files into logical splits. |
# Chapter 3: Shuffle & Reduce — Network Partitioning, Sorting, Reduce Execution & Atomic Output

## 1. Architectural Overview

The Shuffle & Reduce phase is where intermediate key-value pairs produced by mappers are **network-partitioned** (by hash), **sorted/grouped** (by key), and **aggregated** through user-defined reduce functions. This is the culmination of the MapReduce pipeline — raw input data has been transformed into intermediate pairs, and now those pairs must be consolidated into the final output.

**Position in the macro-system:**

```
    Mappers (parallel)
         │
         │ write mr-{mapIdx}-{r} files
         ▼
    ┌────────────────────────────────────────┐
    │         SHUFFLE (implicit via naming)   │
    │  Files mr-*-0   mr-*-1   ... mr-*-{R-1}│
    │  Each reducer reads its own partition   │
    └────────────────────────────────────────┘
         │
         ▼
    ┌────────────────────────────────────────┐
    │         GROUP (in-memory map)           │
    │  keyValues[key] = []string{val1, val2}  │
    └────────────────────────────────────────┘
         │
         ▼
    ┌────────────────────────────────────────┐
    │         REDUCE (per key)                │
    │  reducer.Reduce(key, values, output)    │
    │  Write part-{reduceIdx} to final dir    │
    └────────────────────────────────────────┘
         │
         ▼
    Shared-hdfs/output/final/part-0, part-1, ...
```

**The Coordinator orchestrates the phase transition:** when all map tasks report `TaskCompleted`, the job enters `JobShuffling`. After a configurable educational delay (2 seconds), the Coordinator marks the job as `JobReducing`, and workers requesting tasks begin receiving reduce tasks.

**Design note:** This implementation does not perform a distributed sort across the network. Instead, it relies on the shared filesystem's naming convention (`mr-{mapIdx}-{reduceIdx}`) to route intermediate data, and each reducer does an in-memory grouping by key. This is a pragmatic simplification of the original MapReduce's sort-merge shuffle.

## 2. Component Design & Structural Layout

### Key Abstractions

| Concept | Implementation |
|---|---|
| **Partitioning Function** | `Ihash(key) % R` — FNV-1a hash modulo reduce count |
| **Intermediate File Naming** | `mr-{mapTaskIndex}-{reduceBucketIdx}` — encodes the provenance and partition |
| **Reducer Input Discovery** | Worker scans `output/intermediate/` for files matching `mr-*-{taskIndex}` |
| **Key Grouping** | In-memory `map[string][]string` — loads all values per key from the reducer's partition |
| **Reduce Execution** | Sequential per-key invocation of `Reducer.Reduce(key, valuesChan, outputChan)` |
| **Output Format** | `output/final/part-{reduceTaskIndex}` — TSV: `key\tresult\n` |

### Data Flow Visualization

```text
  Map Task 0              Map Task 1              Map Task 2
    │                        │                       │
    ├─ mr-0-0 (key=a, b)     ├─ mr-1-0 (key=a, c)    ├─ mr-2-0 (key=b, c)
    ├─ mr-0-1 (key=c, d)     ├─ mr-1-1 (key=d)       ├─ mr-2-1 (key=a, d)
    │                        │                       │
    └──────────┬─────────────┴──────────┬────────────┘
               │                        │
               ▼                        ▼
    ┌──────────────────┐    ┌──────────────────┐
    │ Reducer 0         │    │ Reducer 1         │
    │ Reads:            │    │ Reads:            │
    │   mr-0-0          │    │   mr-0-1          │
    │   mr-1-0          │    │   mr-1-1          │
    │   mr-2-0          │    │   mr-2-1          │
    │                   │    │                   │
    │ Group:            │    │ Group:            │
    │   a → [val, val]  │    │   c → [val, val]  │
    │   b → [val, val]  │    │   d → [val, val]  │
    │   c → [val]       │    │   a → [val]       │
    │                   │    │                   │
    │ Reduce each key   │    │ Reduce each key   │
    │ Write part-0      │    │ Write part-1      │
    └──────────────────┘    └──────────────────┘
```

### Output Layout

```
output/
├── intermediate/
│   ├── mr-0-0  (map task 0 → reduce bucket 0)
│   ├── mr-0-1  (map task 0 → reduce bucket 1)
│   ├── mr-1-0  (map task 1 → reduce bucket 0)
│   ├── mr-1-1  (map task 1 → reduce bucket 1)
│   └── ...
└── final/
    ├── part-0  (reducer 0 output)
    └── part-1  (reducer 1 output)
```

## 3. Core Algorithms & Time/Space Complexities

### Shuffle Phase (Implicit via File System) — O(M × R) file operations

The shuffle is not a separate runtime phase in this implementation. It is accomplished by the **partitioning-on-write** pattern in the map phase:

1. Each mapper, while iterating over its `outputChan`, computes `r := Ihash(kv.Key) % NumReduceTasks`.
2. It writes the key-value pair to `files[r]` — one file per reduce bucket.
3. The reducer later reads *all* files matching its bucket index.

**Complexity:** The map-side write is O(K) for K emitted pairs (constant-time hash + file append). The reduce-side read is O(F × L) where F = number of intermediate files and L = average lines per file.

### Reduce Key Grouping — O(N × log S), N = total lines, S = unique keys in partition

```
function groupIntermediateData(partitionIdx):
    keyValues = new map[string][]string
    
    for each file in intermediateDir matching "mr-*-{partitionIdx}":
        for each line in file:
            (key, value) = split(line, "\t")
            keyValues[key].append(value)
    
    return keyValues
```

**Space complexity:** O(N') where N' is the total number of values in the partition. All values are loaded into heap memory before reduction begins.

### Reduce Execution — O(S × V), S = unique keys, V = values per key

```
function executeReduce(keyValues, reducer):
    for each key in sortedKeys(keyValues):
        values = keyValues[key]
        
        valueChan = new chan string(len(values))
        outputChan = new chan string(100)
        
        for each value in values: valueChan <- value
        close(valueChan)
        
        go writer:
            for result in outputChan:
                write finalFile: "{key}\t{result}\n"
        
        reducer.Reduce(key, valueChan, outputChan)
        close(outputChan)
        wait for writer
```

**Note:** Keys are processed in **non-deterministic order** because `map[string][]string` iteration order in Go is randomized. The original MapReduce paper explicitly sorts intermediate keys; this implementation does not, which means output file line order varies between runs.

### Coordinator Shuffle Transition — O(N) task scan

When any map task reports completion, `ReportTask` checks whether *all* map tasks for that job are done. If so:

```
if allMapDone && job.State == JobMapping:
    job.State = JobShuffling
    go func() {
        time.Sleep(2 * time.Second)  // educational visualization window
        lock()
        if job.State == JobShuffling:
            job.State = JobReducing
        unlock()
    }()
```

This 2-second hold allows a web dashboard to observe the SHUFFLING state before reduce tasks begin.

## 4. Code Architecture & Reference

```go
// worker/main.go — ExecuteReduceTask (the full reduce pipeline)
func (w *Worker) ExecuteReduceTask(task *shared.Task, job *shared.Job,
    reducerCode []byte, inputPath, outputPath string) error {

    w.ReportTask(task.ID, job.ID, shared.TaskRunning, "")

    reducer, _ := getReducerByName(job.Reducer)

    // 1. Read intermediate files matching this reduce partition
    intermediateDir := filepath.Join(outputPath, "intermediate")
    files, _ := ioutil.ReadDir(intermediateDir)

    keyValues := make(map[string][]string)
    suffix := fmt.Sprintf("-%d", task.TaskIndex)

    for _, file := range files {
        if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), suffix) {
            content, _ := ioutil.ReadFile(filepath.Join(intermediateDir, file.Name()))
            for _, line := range strings.Split(string(content), "\n") {
                parts := strings.SplitN(line, "\t", 2)
                if len(parts) == 2 {
                    keyValues[parts[0]] = append(keyValues[parts[0]], parts[1])
                }
            }
        }
    }

    // 2. Write final output file
    outputDir := filepath.Join(outputPath, "final")
    os.MkdirAll(outputDir, 0755)
    outputFilePath := filepath.Join(outputDir, fmt.Sprintf("part-%d", task.TaskIndex))
    outputFile, _ := os.Create(outputFilePath)
    defer outputFile.Close()

    // 3. Reduce each key
    for key, values := range keyValues {
        valueChan := make(chan string, len(values))
        for _, v := range values {
            valueChan <- v
        }
        close(valueChan)

        outputChan := make(chan string, 100)
        var wg sync.WaitGroup
        wg.Add(1)
        go func() {
            defer wg.Done()
            for result := range outputChan {
                fmt.Fprintf(outputFile, "%s\t%s\n", key, result)
            }
        }()

        reducer.Reduce(key, valueChan, outputChan)
        close(outputChan)
        wg.Wait()
    }

    w.ReportTask(task.ID, job.ID, shared.TaskCompleted, "")
    return nil
}
```

## 5. Trade-offs & Concurrency Considerations

### Thread Safety & Sync

- **No cross-reducer synchronization.** Each reduce task operates independently — there is no shared mutable state between reducers. The Coordinator's mutex is only touched during the initial `ReportTask` calls.
- **Per-key channel pipeline.** Each key's reduction uses a dedicated goroutine pair (value feeder + result writer) synchronized by channel closures and `sync.WaitGroup`. Because keys are processed sequentially, there is no concurrent map mutation in `keyValues`.
- **File I/O is not atomic.** The output file is written directly to its final path. If a crash occurs mid-write, a partially written `part-N` file may persist. The original MapReduce paper uses atomic file rename (`mv tmp out`) to ensure outputs are either complete or absent.

### Engineering Bottlenecks

| Bottleneck | Impact |
|---|---|
| **No distributed sort** | The original MapReduce guarantees that each reducer receives keys in sorted order. This implementation skips sorting entirely, meaning output order is non-deterministic and combiners (map-side reducers) are harder to implement correctly. |
| **In-memory grouping O(N')** | All values for the partition are loaded into heap before any reduction starts. For a reducer handling millions of values for a single key, this can OOM. A production system would spill to disk or use an external merge sort. |
| **No combiner support** | The original MapReduce allows a "Combiner" function (local reduce on the mapper side) to reduce network I/O. This implementation has no combiner abstraction — all pairs are written to intermediate files in full. |
| **Sequential key processing** | Each key's values are reduced one at a time. For partitions with many keys, there is no parallelism within a reduce task. A production system would process batches of keys in parallel goroutines, bounded by a semaphore. |
| **No atomic output rename** | `ExecuteReduceTask` writes directly to `output/final/part-N`. If the Coordinator reassigns a reduce task to a new worker after a crash, the output file may have duplicate or interleaved content from two workers. An atomic rename (`tmp → final`) would guarantee exactly-once semantics. |
| **Full-file reads for intermediate data** | Each intermediate file is read entirely into memory with `ioutil.ReadFile`. For large intermediate files, this duplicates the data between the `ReadFile` buffer and the `keyValues` map. A streaming line reader (`bufio.Scanner`) would reduce peak memory. |
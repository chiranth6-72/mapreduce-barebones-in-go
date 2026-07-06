# Chapter 1: The Coordinator — Job Lifecycle, Task Scheduling & Fault Tolerance

## 1. Architectural Overview

The Coordinator is the single source of truth in this MapReduce cluster. It acts as both a **scheduler** (handing out map/reduce tasks to workers based on availability) and a **state machine** (transitioning jobs through the canonical MapReduce phases: PENDING → MAPPING → SHUFFLING → REDUCING → COMPLETED/FAILED).

**Position in the macro-system:**

```
         [Browser Dashboard] ← JSON API / HTTP
                │
         ┌──────┴──────┐
         │ Coordinator │  (RPC server on :1234, Web UI on :8080)
         └──────┬──────┘
                │ net/rpc (TCP)
        ┌───────┼───────┐
        │       │       │
    [Worker] [Worker] [Worker]  ← poll for tasks, report status
        │       │       │
        └───────┼───────┘
          [shared-hdfs/]     ← shared filesystem (simulated GFS)
```

The Coordinator never executes user code. Its responsibilities are:
- **RPC service registration** — exports `CoordinatorRPC` service on a TCP listener
- **Worker lifecycle** — handles registration, heartbeat reception, expiry detection
- **Task state machine** — transitions tasks through PENDING → ASSIGNED → RUNNING → COMPLETED/FAILED
- **Job pipeline orchestration** — blocks reduce assignment until all map tasks complete; inserts a synthetic SHUFFLING phase for educational visualization
- **Fault tolerance** — reassigns any task held by a worker that fails its heartbeat deadline (3× interval)

## 2. Component Design & Structural Layout

### Key Abstractions

| Structure | Role |
|---|---|
| `Coordinator` | Central state holder; owns maps of jobs, tasks, workers, and serialized code |
| `CoordinatorRPC` | Thin RPC wrapper over `Coordinator` methods (enables `rpc.RegisterName`) |
| `Job` | Describes one MapReduce job: I/O paths, mapper/reducer names, task counts, lifecycle state |
| `Task` | A single unit of work (map or reduce) with lifecycle tracking |
| `WorkerInfo` | Worker metadata: address, last-seen timestamp, active flag, tasks handled count |

### State Representation — Job Lifecycle

```
     ┌──────────┐
     │ PENDING  │  ← job submitted, tasks created
     └────┬─────┘
          │ (first map task assigned)
          v
     ┌──────────┐
     │ MAPPING  │  ← all map tasks are in flight or pending
     └────┬─────┘
          │ (all map tasks COMPLETED)
          v
     ┌────────────┐
     │ SHUFFLING  │  ← synthetic 2-second hold for visualization
     └────┬───────┘
          │ (timer fires or worker requests task)
          v
     ┌──────────┐
     │ REDUCING │  ← reduce tasks being assigned/executed
     └────┬─────┘
          │ (all reduce tasks COMPLETED)
          v
     ┌─────────────┐
     │ COMPLETED   │
     └─────────────┘

     Any task failure → FAILED (terminal)
```

### Data Format — In-Memory Maps

```text
Coordinator (protected by sync.Mutex)
├── jobs:        map[string]*Job           keyed by job ID
├── mapTasks:    map[string]*Task          keyed by task ID
├── reduceTasks: map[string]*Task          keyed by task ID
├── workers:     map[string]*WorkerInfo    keyed by worker ID
├── mapperCode:  map[string][]byte         jobID → gob-encoded Mapper
└── reducerCode: map[string][]byte         jobID → gob-encoded Reducer
```

## 3. Core Algorithms & Time/Space Complexities

### `RequestTask` — Task Assignment (O(n) in pending tasks)

The Coordinator assigns tasks to a requesting worker with strict priority ordering:

1. **Map first, Reduce second.** Workers are never given a reduce task until *every* map task for the job has reached `TaskCompleted`.
2. **Linear scan** of the map tasks map to find the first `TaskPending` entry. Same for reduce tasks once maps are drained.
3. **Job state promotion:** assigning the first map task promotes the job from `JobPending` → `JobMapping`; assigning the first reduce task promotes it from `JobShuffling` → `JobReducing`.

```
RequestTask(workerID):
  update worker.LastSeen = now
  
  for each task in mapTasks:
    if task.State == PENDING:
      task.State = ASSIGNED
      task.WorkerID = workerID
      if job.State == PENDING: job.State = MAPPING
      return task
  
  if all mapTasks are COMPLETED:
    for each task in reduceTasks:
      if task.State == PENDING:
        task.State = ASSIGNED
        task.WorkerID = workerID
        if job.State ∈ {MAPPING, SHUFFLING}: job.State = REDUCING
        return task
  
  return nil  // no work available
```

**Complexity:** O(N) where N = number of tasks of the relevant type. A production system would use priority queues or idle-task indexes.

### `ReportTask` — State Transition Handler (O(N) in tasks)

Handles incoming status updates and drives the job completion check:

1. Locate the task by ID (in mapTasks or reduceTasks).
2. Update its state, error, and timestamps.
3. On `TaskCompleted`:
   - Check if **all** map tasks for the job are done → promote job to `JobShuffling` (and schedule a goroutine to auto-promote to `JobReducing` after 2s).
   - Check if **all** reduce tasks for the job are done → promote job to `JobCompleted`.
4. On `TaskFailed` → promote job to `JobFailed` (terminal).

### Heartbeat & Worker Expiry (O(W) periodic, W = workers)

Every 3 seconds, `StartMonitoring` iterates all workers. If `time.Since(worker.LastSeen) > 9s` (3× interval), the worker is marked inactive. Then *every* task assigned to that worker (both map and reduce) is reset to `TaskPending` with an empty `WorkerID`, making it eligible for reassignment.

### `SubmitJob` — Task Generation (O(M + R))

Creates M map tasks (indexed 0..M-1) and R reduce tasks (indexed 0..R-1) for the job, all initially `TaskPending`. Defaults: M=4, R=2.

### `listFilesRecursive` — File Explorer (O(F), F = files in storage tree)

Recursively walks the `shared-hdfs/` directory to produce a flat JSON list of all files and directories for the web dashboard's file explorer.

## 4. Code Architecture & Reference

```go
// coordinator/main.go — Core Coordinator struct
type Coordinator struct {
    mu           sync.Mutex
    jobs         map[string]*shared.Job
    mapTasks     map[string]*shared.Task
    reduceTasks  map[string]*shared.Task
    workers      map[string]*shared.WorkerInfo
    mapperCode   map[string][]byte // jobID -> gob-encoded Mapper
    reducerCode  map[string][]byte // jobID -> gob-encoded Reducer
    nextTaskID   int
    storagePath  string
    rpcPort      string
    webPort      string
}

// CoordinatorRPC — exported as "CoordinatorRPC" over net/rpc
type CoordinatorRPC struct {
    coord *Coordinator
}

// RPC Methods exposed by the Coordinator service:
//   SubmitJob(job, mapperCode, reducerCode) → JobID
//   GetJobStatus(JobID) → Job
//   RequestTask(WorkerID) → Task, Job, Mapper/Reducer code, paths
//   ReportTask(TaskID, State, Error) → Success
//   RegisterWorker(WorkerID, Address) → Success
//   Heartbeat(WorkerID) → Success
```

**RPC Server Initialization:**
```go
rpcServer := rpc.NewServer()
rpcServer.RegisterName("CoordinatorRPC", &CoordinatorRPC{coord: c})
listener, _ := net.Listen("tcp", ":1234")
go func() {
    for {
        conn, _ := listener.Accept()
        go rpcServer.ServeConn(conn)
    }
}()
```

## 5. Trade-offs & Concurrency Considerations

### Thread Safety & Sync

- **Single `sync.Mutex`** protects all Coordinator state. Every RPC handler (`SubmitJob`, `RequestTask`, `ReportTask`, `Heartbeat`, `RegisterWorker`) and the monitoring goroutine grab `c.mu.Lock()` before mutating any map.
- **Goroutine safety in `ReportTask`:** The 2-second SHUFFLING → REDUCING transition is launched as `go func() { time.Sleep(2s); c.mu.Lock(); ...; c.mu.Unlock() }()`. This goroutine contends for the same mutex, so the transition is correctly serialized against concurrent task assignments.
- **No deadlocks** because all lock acquisitions are simple (non-nested) — no goroutine holds `c.mu` while calling back into another locked method.

### Engineering Bottlenecks

| Bottleneck | Impact |
|---|---|
| **Single mutex contention** | All RPC handlers and the monitor serialize on one lock. Under heavy load (many workers polling simultaneously), this becomes a throughput bottleneck. A production design would use fine-grained locks (per-job or per-task sharding). |
| **O(N) linear task scanning** | `RequestTask` and `ReportTask` both iterate entire task maps. With thousands of tasks, this adds latency. A priority queue or free-list of pending tasks would reduce to O(1). |
| **In-memory only** | No persistence to disk. If the Coordinator crashes, all job state is lost. A production system would write-ahead log (WAL) to etcd/ZooKeeper or a replicated database. |
| **Single point of failure** | There is exactly one Coordinator. This is the canonical limitation of the master-worker pattern. Recovery requires restarting from scratch. |
| **No anti-entropy** | If a worker is partitioned but not dead (zombie worker), it may continue processing a task that was reassigned. The system has no fencing mechanism (e.g., epoch IDs) to prevent duplicate writes. |
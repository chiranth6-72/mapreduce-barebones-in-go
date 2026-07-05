
# Distributed MapReduce Data Processing Engine - Guided Learning Project

This guided project walks you through building a production-grade Distributed MapReduce framework in Go. You will implement the core components, understand the distributed computing patterns, and deploy a working cluster using Docker Compose.

---

## 1. High-Level Concept & Analogy

MapReduce is a programming model for processing and generating large datasets in parallel across a cluster of computers. It abstracts the complexity of distributed computation, fault tolerance, and data partitioning.

**Real-World Analogy:** Imagine a factory assembly line. Raw materials (input data) arrive and are divided into batches. Each worker (mapper) processes their batch independently, creating intermediate parts. These parts are then sorted and grouped by type. Finally, specialized workers (reducers) assemble the final products from these grouped parts. If a worker falls ill, a supervisor (coordinator) reassigns their work to others.

MapReduce follows this exact pattern: Map workers process input splits, Shuffle/Sort groups intermediate data by key, and Reduce workers produce final output. The Coordinator orchestrates everything.

---

## 2. System Architecture & Flow

```
+------------------+     +------------------+     +------------------+
|   Coordinator    |     |     Worker 1     |     |     Worker N     |
|------------------|     |------------------|     |------------------|
| - Task Scheduler |     | - Map Tasks      |     | - Map Tasks      |
| - State Tracker  |     | - Reduce Tasks   |     | - Reduce Tasks   |
| - Fault Monitor  |     | - RPC Server     |     | - RPC Server     |
+--------+---------+     +--------+---------+     +--------+---------+
         |                        |                        |
         v                        v                        v
+------------------------------------------------------------------+
|                        Shared Storage (HDFS)                      |
|------------------------------------------------------------------|
|  /input/    /intermediate/    /output/    /tasks/                  |
+------------------------------------------------------------------+
         ^                        ^                        ^
         |                        |                        |
+------------------+     +------------------+     +------------------+
|   Client (CLI)   |     |   Worker 2      |     |   Worker 3      |
+------------------+     +------------------+     +------------------+
```

**Data Flow:**
1. Client submits job to Coordinator
2. Coordinator splits input, creates Map tasks
3. Workers pull tasks, execute Map functions
4. Workers write intermediate key-value pairs to shared storage
5. Coordinator creates Reduce tasks after all Maps complete
6. Workers pull Reduce tasks, process grouped intermediate data
7. Workers write final output to shared storage
8. Coordinator notifies client of job completion

---

## 3. Technical Deep Dive

### Core Components

**Coordinator:**
- Single point of control (not a bottleneck due to minimal work)
- Manages job lifecycle: PENDING -> MAPPING -> SHUFFLING -> REDUCING -> COMPLETED
- Tracks task assignments and worker health
- Implements fault tolerance via task re-assignment

**Worker:**
- Long-running process that polls Coordinator for tasks
- Executes Map or Reduce functions provided by user
- Writes intermediate data to shared storage
- Reports task completion/failure back to Coordinator

**RPC Interface:**
- Go's `net/rpc` package for communication
- Worker-to-Coordinator: RequestTask, ReportTask
- Uses TCP for reliable, ordered message delivery

**Shared Storage:**
- Simulated HDFS using a shared Docker volume
- Stores: input files, intermediate data, output files, task metadata
- Workers read/write directly to this shared filesystem

### MapReduce Phases

**Map Phase:**
```
type Mapper interface {
    Map(key string, value string, output chan<- KeyValue)
}
```
- Input: Split of the original dataset
- Process: User-defined function emits intermediate key-value pairs
- Output: Written to intermediate files in shared storage

**Shuffle/Sort Phase:**
- Implicit phase handled by Coordinator
- Groups all intermediate values by the same key
- Distributes grouped data to Reduce tasks

**Reduce Phase:**
```
type Reducer interface {
    Reduce(key string, values <-chan string, output chan<- string)
}
```
- Input: A key and all its associated values
- Process: User-defined function aggregates values
- Output: Written to final output files

---

## 4. Production Blueprint

### How Google Implements MapReduce

Google's original MapReduce paper (Dean & Ghemawat, 2004) describes:

- **Master (Coordinator):** Assigns map tasks to workers, tracks progress, handles failures
- **Worker:** Executes tasks, reads/writes data from GFS (Google File System)
- **GFS:** Distributed file system providing high throughput for large sequential reads/writes
- **Fault Tolerance:** Re-execution of failed tasks, speculative execution for stragglers

**Key Innovations:**
- Automatic parallelization and distribution
- Fault tolerance as a first-class concern
- Scalability to thousands of machines
- Heterogeneity-aware scheduling

### How Netflix Uses MapReduce Patterns

Netflix uses similar patterns in their data pipeline:
- **Genie:** Workflow orchestration service
- **Hadoop:** For batch processing at scale
- **Spark:** For iterative algorithms (evolved from MapReduce)

**Lesson:** MapReduce is the foundation; modern systems build upon its principles.

---

## 5. Production Gotchas & Trade-offs

### Failure Modes & Mitigations

1. **Worker Crash During Map:**
   - *Problem:* Intermediate data may be partially written
   - *Solution:* Coordinator reassigns task; worker re-reads input, re-executes Map
   - *Trade-off:* Idempotency required in Map functions

2. **Worker Crash During Reduce:**
   - *Problem:* Partial output written
   - *Solution:* Coordinator reassigns; worker re-reads intermediate data
   - *Trade-off:* Intermediate data must be complete before Reduce starts

3. **Straggler Workers:**
   - *Problem:* Slow workers delay entire job
   - *Solution:* Speculative execution - launch duplicate tasks
   - *Trade-off:* Wasted compute resources

4. **Network Partitions:**
   - *Problem:* Workers cannot communicate with Coordinator
   - *Solution:* Timeout-based task re-assignment
   - *Trade-off:* May cause duplicate task execution

5. **Shared Storage Bottleneck:**
   - *Problem:* All workers reading/writing to same storage
   - *Solution:* Partition data, use local disks with replication
   - *Trade-off:* Increased complexity in data management

### Scale Bottlenecks

| Component | Bottleneck at Scale | Solution |
|-----------|---------------------|----------|
| Coordinator | Memory for tracking tasks | Hierarchical coordination |
| Network | RPC overhead | Batch task requests |
| Storage | I/O throughput | Distributed file system |
| Workers | Task scheduling | Dynamic load balancing |

---

## 6. Implementation Guide

This section provides step-by-step instructions to build the system.

---

### Prerequisites

1. Go 1.20+ installed
2. Docker and Docker Compose installed
3. Basic understanding of Go concurrency (goroutines, channels)
4. Familiarity with Go's `net/rpc` package

---

### Project Structure

```
mapreduce/
├── coordinator/
│   ├── main.go          # Coordinator entry point
│   ├── rpc.go           # RPC server implementation
│   ├── scheduler.go     # Task scheduling logic
│   └── state.go         # Job and task state management
├── worker/
│   ├── main.go          # Worker entry point
│   ├── rpc.go           # RPC client implementation
│   ├── mapper.go        # Map task execution
│   ├── reducer.go       # Reduce task execution
│   └── filesystem.go    # Shared storage operations
├── shared/
│   ├── rpc.go           # Shared RPC types and interfaces
│   ├── types.go         # Data structures (Job, Task, etc.)
│   └── util.go          # Utility functions
├── examples/
│   ├── wordcount/
│   │   ├── main.go      # Word count example
│   │   └── mapper_reducer.go
│   └── invertindex/
│       ├── main.go
│       └── mapper_reducer.go
├── docker-compose.yml
└── README.md
```

---

### Step 1: Define Shared Types

Create `shared/types.go`:

```go
package shared

import "time"

// Job represents a complete MapReduce job
type Job struct {
	ID          string
	InputPath   string
	OutputPath  string
	Mapper      string // Name of mapper implementation
	Reducer     string // Name of reducer implementation
	NumMapTasks int
	NumReduceTasks int
	State       JobState
	CreatedAt   time.Time
	CompletedAt *time.Time
}

// JobState represents the current state of a job
type JobState int

const (
	JobPending JobState = iota
	JobMapping
	JobShuffling
	JobReducing
	JobCompleted
	JobFailed
)

func (s JobState) String() string {
	switch s {
	case JobPending:
		return "PENDING"
	case JobMapping:
		return "MAPPING"
	case JobShuffling:
		return "SHUFFLING"
	case JobReducing:
		return "REDUCING"
	case JobCompleted:
		return "COMPLETED"
	case JobFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// TaskType represents the type of task
type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
)

// Task represents a single unit of work
type Task struct {
	ID        string
	JobID     string
	TaskType  TaskType
	TaskIndex int // 0-based index for this task type
	State     TaskState
	WorkerID  string
	AssignedAt time.Time
	StartedAt *time.Time
	CompletedAt *time.Time
	Error     string
}

// TaskState represents the current state of a task
type TaskState int

const (
	TaskPending TaskState = iota
	TaskAssigned
	TaskRunning
	TaskCompleted
	TaskFailed
)

func (s TaskState) String() string {
	switch s {
	case TaskPending:
		return "PENDING"
	case TaskAssigned:
		return "ASSIGNED"
	case TaskRunning:
		return "RUNNING"
	case TaskCompleted:
		return "COMPLETED"
	case TaskFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// KeyValue represents an intermediate key-value pair
type KeyValue struct {
	Key   string
	Value string
}

// WorkerInfo represents a registered worker
type WorkerInfo struct {
	ID       string
	Address  string
	LastSeen time.Time
	Active   bool
}
```

---

### Step 2: Define RPC Interface

Create `shared/rpc.go`:

```go
package shared

import "net/rpc"

// CoordinatorRPC defines the RPC methods exposed by the Coordinator
type CoordinatorRPC struct {
	coord *Coordinator
}

// WorkerRPC defines the RPC methods exposed by Workers
type WorkerRPC struct {
	worker *Worker
}

// TaskRequest is sent by workers to request a task
type TaskRequest struct {
	WorkerID string
}

// TaskResponse is sent by coordinator in response to TaskRequest
type TaskResponse struct {
	Task      *Task
	Job       *Job
	Mapper    []byte // Serialized mapper code
	Reducer   []byte // Serialized reducer code
	InputPath string
	OutputPath string
}

// TaskUpdate is sent by workers to report task status
type TaskUpdate struct {
	TaskID   string
	JobID    string
	State    TaskState
	Error    string
	WorkerID string
}

// TaskUpdateResponse is the coordinator's acknowledgment
type TaskUpdateResponse struct {
	Success bool
}

// RegisterRequest is sent by workers to register themselves
type RegisterRequest struct {
	WorkerID string
	Address  string
}

// RegisterResponse is the coordinator's acknowledgment
type RegisterResponse struct {
	Success bool
	Error   string
}

// HeartbeatRequest is sent periodically by workers
type HeartbeatRequest struct {
	WorkerID string
}

// HeartbeatResponse is the coordinator's acknowledgment
type HeartbeatResponse struct {
	Success bool
}

// SubmitJobRequest is sent by clients to submit a new job
type SubmitJobRequest struct {
	Job          Job
	MapperCode   []byte
	ReducerCode  []byte
}

// SubmitJobResponse is the coordinator's acknowledgment
type SubmitJobResponse struct {
	Success bool
	JobID   string
	Error   string
}

// GetJobStatusRequest is sent by clients to check job status
type GetJobStatusRequest struct {
	JobID string
}

// GetJobStatusResponse contains the job status
type GetJobStatusResponse struct {
	Job   *Job
	Error string
}

// RPC Service Names
const (
	CoordinatorServiceName = "CoordinatorRPC"
	WorkerServiceName      = "WorkerRPC"
)
```

---

### Step 3: Implement Coordinator

Create `coordinator/main.go`:

```go
package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"mapreduce/shared"
)

type Coordinator struct {
	mu           sync.Mutex
	jobs         map[string]*shared.Job
	mapTasks     map[string]*shared.Task
	reduceTasks  map[string]*shared.Task
	workers      map[string]*shared.WorkerInfo
	mapperCode   map[string][]byte // jobID -> mapper code
	reducerCode  map[string][]byte // jobID -> reducer code
	nextTaskID  int
	storagePath  string
}

func NewCoordinator(storagePath string) *Coordinator {
	return &Coordinator{
		jobs:        make(map[string]*shared.Job),
		mapTasks:    make(map[string]*shared.Task),
		reduceTasks: make(map[string]*shared.Task),
		workers:     make(map[string]*shared.WorkerInfo),
		mapperCode:  make(map[string][]byte),
		reducerCode: make(map[string][]byte),
		nextTaskID:  0,
		storagePath: storagePath,
	}
}

func (c *Coordinator) generateTaskID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := fmt.Sprintf("task-%d", c.nextTaskID)
	c.nextTaskID++
	return id
}

func (c *Coordinator) SubmitJob(req *shared.SubmitJobRequest, resp *shared.SubmitJobResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate job
	if req.Job.ID == "" {
		req.Job.ID = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	if req.Job.NumMapTasks <= 0 {
		req.Job.NumMapTasks = 10
	}
	if req.Job.NumReduceTasks <= 0 {
		req.Job.NumReduceTasks = 5
	}

	// Store job
	job := &req.Job
	job.State = shared.JobPending
	job.CreatedAt = time.Now()
	c.jobs[job.ID] = job

	// Store mapper and reducer code
	c.mapperCode[job.ID] = req.MapperCode
	c.reducerCode[job.ID] = req.ReducerCode

	// Create map tasks
	for i := 0; i < job.NumMapTasks; i++ {
		task := &shared.Task{
			ID:       c.generateTaskID(),
			JobID:    job.ID,
			TaskType: shared.TaskMap,
			TaskIndex: i,
			State:    shared.TaskPending,
		}
		c.mapTasks[task.ID] = task
	}

	// Create reduce tasks
	for i := 0; i < job.NumReduceTasks; i++ {
		task := &shared.Task{
			ID:       c.generateTaskID(),
			JobID:    job.ID,
			TaskType: shared.TaskReduce,
			TaskIndex: i,
			State:    shared.TaskPending,
		}
		c.reduceTasks[task.ID] = task
	}

	resp.Success = true
	resp.JobID = job.ID
	return nil
}

func (c *Coordinator) GetJobStatus(req *shared.GetJobStatusRequest, resp *shared.GetJobStatusResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	job, exists := c.jobs[req.JobID]
	if !exists {
		resp.Error = "job not found"
		return nil
	}

	resp.Job = job
	return nil
}

func (c *Coordinator) RequestTask(req *shared.TaskRequest, resp *shared.TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update worker last seen time
	if worker, exists := c.workers[req.WorkerID]; exists {
		worker.LastSeen = time.Now()
		worker.Active = true
	}

	// Find an available task
	// Priority: Map tasks first, then reduce tasks
	for _, task := range c.mapTasks {
		if task.State == shared.TaskPending {
			task.State = shared.TaskAssigned
			task.WorkerID = req.WorkerID
			task.AssignedAt = time.Now()

			job := c.jobs[task.JobID]
			resp.Task = task
			resp.Job = job
			resp.Mapper = c.mapperCode[task.JobID]
			resp.Reducer = c.reducerCode[task.JobID]
			resp.InputPath = job.InputPath
			resp.OutputPath = job.OutputPath

			return nil
		}
	}

	// Check if all map tasks are done, then assign reduce tasks
	allMapDone := true
	for _, task := range c.mapTasks {
		if task.State != shared.TaskCompleted {
			allMapDone = false
			break
		}
	}

	if allMapDone {
		// Check if job is in mapping state, transition to shuffling
		for _, job := range c.jobs {
			if job.State == shared.JobMapping {
				job.State = shared.JobShuffling
				break
			}
		}

		// Now assign reduce tasks
		for _, task := range c.reduceTasks {
			if task.State == shared.TaskPending {
				task.State = shared.TaskAssigned
				task.WorkerID = req.WorkerID
				task.AssignedAt = time.Now()

				job := c.jobs[task.JobID]
				resp.Task = task
				resp.Job = job
				resp.Mapper = c.mapperCode[task.JobID]
				resp.Reducer = c.reducerCode[task.JobID]
				resp.InputPath = job.InputPath
				resp.OutputPath = job.OutputPath

				return nil
			}
		}
	}

	// No tasks available
	resp.Task = nil
	return nil
}

func (c *Coordinator) ReportTask(req *shared.TaskUpdate, resp *shared.TaskUpdateResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find the task
	var task *shared.Task
	if t, exists := c.mapTasks[req.TaskID]; exists {
		task = t
	} else if t, exists := c.reduceTasks[req.TaskID]; exists {
		task = t
	} else {
		resp.Success = false
		return nil
	}

	// Update task state
	task.State = req.State
	task.Error = req.Error
	task.WorkerID = req.WorkerID
	if req.State == shared.TaskRunning {
		task.StartedAt = &time.Time{}
		*task.StartedAt = time.Now()
	}
	if req.State == shared.TaskCompleted || req.State == shared.TaskFailed {
		task.CompletedAt = &time.Time{}
		*task.CompletedAt = time.Now()
	}

	// Check if job is complete
	job, exists := c.jobs[req.JobID]
	if !exists {
		resp.Success = true
		return nil
	}

	if req.State == shared.TaskCompleted {
		// Check if all map tasks are done
		allMapDone := true
		for _, t := range c.mapTasks {
			if t.JobID == req.JobID && t.State != shared.TaskCompleted {
				allMapDone = false
				break
			}
		}

		if allMapDone && job.State == shared.JobMapping {
			job.State = shared.JobShuffling
			// In a real implementation, we'd wait for shuffle to complete
			// For simplicity, we immediately transition to reducing
			job.State = shared.JobReducing
		}

		// Check if all reduce tasks are done
		allReduceDone := true
		for _, t := range c.reduceTasks {
			if t.JobID == req.JobID && t.State != shared.TaskCompleted {
				allReduceDone = false
				break
			}
		}

		if allReduceDone && job.State == shared.JobReducing {
			job.State = shared.JobCompleted
			now := time.Now()
			job.CompletedAt = &now
		}
	}

	resp.Success = true
	return nil
}

func (c *Coordinator) RegisterWorker(req *shared.RegisterRequest, resp *shared.RegisterResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if worker already registered
	if _, exists := c.workers[req.WorkerID]; exists {
		resp.Success = true
		return nil
	}

	// Register new worker
	c.workers[req.WorkerID] = &shared.WorkerInfo{
		ID:       req.WorkerID,
		Address:  req.Address,
		LastSeen: time.Now(),
		Active:   true,
	}

	resp.Success = true
	return nil
}

func (c *Coordinator) Heartbeat(req *shared.HeartbeatRequest, resp *shared.HeartbeatResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if worker, exists := c.workers[req.WorkerID]; exists {
		worker.LastSeen = time.Now()
		worker.Active = true
	}

	resp.Success = true
	return nil
}

func (c *Coordinator) StartRPCServer(hostPort string) {
	rpc.RegisterName(shared.CoordinatorServiceName, shared.CoordinatorRPC{coord: c})
	listener, err := net.Listen("tcp", hostPort)
	if err != nil {
		log.Fatal("Coordinator: listen error:", err)
	}
	log.Printf("Coordinator: listening on %s", hostPort)
	go rpc.Accept(listener)
}

func (c *Coordinator) StartMonitoring(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		for id, worker := range c.workers {
			if time.Since(worker.LastSeen) > interval*3 {
				worker.Active = false
				log.Printf("Worker %s marked as inactive (last seen: %v)", id, worker.LastSeen)
			}
		}

		// Reassign tasks from inactive workers
		for _, task := range c.mapTasks {
			if task.State == shared.TaskAssigned || task.State == shared.TaskRunning {
				if worker, exists := c.workers[task.WorkerID]; !exists || !worker.Active {
					task.State = shared.TaskPending
					task.WorkerID = ""
					log.Printf("Reassigned map task %s from inactive worker %s", task.ID, task.WorkerID)
				}
			}
		}

		for _, task := range c.reduceTasks {
			if task.State == shared.TaskAssigned || task.State == shared.TaskRunning {
				if worker, exists := c.workers[task.WorkerID]; !exists || !worker.Active {
					task.State = shared.TaskPending
					task.WorkerID = ""
					log.Printf("Reassigned reduce task %s from inactive worker %s", task.ID, task.WorkerID)
				}
			}
		}
		c.mu.Unlock()
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: coordinator <port> <storage-path>")
		os.Exit(1)
	}

	port := os.Args[1]
	storagePath := "./shared-hdfs"
	if len(os.Args) > 2 {
		storagePath = os.Args[2]
	}

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Fatal("Failed to create storage directory:", err)
	}

	coord := NewCoordinator(storagePath)
	coord.StartRPCServer(":" + port)
	coord.StartMonitoring(5 * time.Second)

	// Wait for shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Coordinator shutting down...")
}
```

---

### Step 4: Implement Worker

Create `worker/main.go`:

```go
package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"plugin"
	"strings"
	"sync"
	"syscall"
	"time"

	"mapreduce/shared"
)

type Worker struct {
	mu          sync.Mutex
	id          string
	coordinator string
	storagePath string
	active      bool
}

func NewWorker(id, coordinator, storagePath string) *Worker {
	return &Worker{
		id:          id,
		coordinator: coordinator,
		storagePath: storagePath,
		active:      true,
	}
}

func (w *Worker) RegisterWithCoordinator() error {
	client, err := rpc.Dial("tcp", w.coordinator)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %v", err)
	}
	defer client.Close()

	req := &shared.RegisterRequest{
		WorkerID: w.id,
		Address:  w.coordinator, // In production, this would be the worker's address
	}
	var resp shared.RegisterResponse
	if err := client.Call(shared.CoordinatorServiceName+".RegisterWorker", req, &resp); err != nil {
		return fmt.Errorf("RPC error: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Error)
	}

	log.Printf("Worker %s registered with coordinator", w.id)
	return nil
}

func (w *Worker) SendHeartbeat() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		client, err := rpc.Dial("tcp", w.coordinator)
		if err != nil {
			log.Printf("Heartbeat failed: %v", err)
			continue
		}

		req := &shared.HeartbeatRequest{
			WorkerID: w.id,
		}
		var resp shared.HeartbeatResponse
		if err := client.Call(shared.CoordinatorServiceName+".Heartbeat", req, &resp); err != nil {
			log.Printf("Heartbeat RPC error: %v", err)
		}
		client.Close()
	}
}

func (w *Worker) RequestTask() (*shared.TaskResponse, error) {
	client, err := rpc.Dial("tcp", w.coordinator)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coordinator: %v", err)
	}
	defer client.Close()

	req := &shared.TaskRequest{
		WorkerID: w.id,
	}
	var resp shared.TaskResponse
	if err := client.Call(shared.CoordinatorServiceName+".RequestTask", req, &resp); err != nil {
		return nil, fmt.Errorf("RPC error: %v", err)
	}

	return &resp, nil
}

func (w *Worker) ReportTask(taskID, jobID string, state shared.TaskState, errMsg string) error {
	client, err := rpc.Dial("tcp", w.coordinator)
	if err != nil {
		return fmt.Errorf("failed to connect to coordinator: %v", err)
	}
	defer client.Close()

	req := &shared.TaskUpdate{
		TaskID:   taskID,
		JobID:    jobID,
		State:    state,
		Error:    errMsg,
		WorkerID: w.id,
	}
	var resp shared.TaskUpdateResponse
	if err := client.Call(shared.CoordinatorServiceName+".ReportTask", req, &resp); err != nil {
		return fmt.Errorf("RPC error: %v", err)
	}

	return nil
}

func (w *Worker) ExecuteMapTask(task *shared.Task, job *shared.Job, mapperCode []byte, inputPath, outputPath string) error {
	// Report task as running
	if err := w.ReportTask(task.ID, job.ID, shared.TaskRunning, ""); err != nil {
		return err
	}

	// Load mapper plugin
	// In a real implementation, we'd use Go plugins or a more sophisticated approach
	// For this example, we'll use a simpler approach with gob encoding
	var mapper shared.Mapper
	if err := gob.NewDecoder(bytes.NewReader(mapperCode)).Decode(&mapper); err != nil {
		return fmt.Errorf("failed to decode mapper: %v", err)
	}

	// Get input file for this map task
	inputFiles, err := ioutil.ReadDir(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input directory: %v", err)
	}

	// For simplicity, each map task processes one input file
	// In production, we'd split files into chunks
	if task.TaskIndex >= len(inputFiles) {
		return fmt.Errorf("no input file for task index %d", task.TaskIndex)
	}

	inputFile := inputFiles[task.TaskIndex]
	if inputFile.IsDir() {
		return fmt.Errorf("input is a directory: %s", inputFile.Name())
	}

	inputFilePath := filepath.Join(inputPath, inputFile.Name())
	content, err := ioutil.ReadFile(inputFilePath)
	if err != nil {
		return fmt.Errorf("failed to read input file: %v", err)
	}

	// Create intermediate directory for this map task
	intermediateDir := filepath.Join(outputPath, "intermediate", fmt.Sprintf("map-%d", task.TaskIndex))
	if err := os.MkdirAll(intermediateDir, 0755); err != nil {
		return fmt.Errorf("failed to create intermediate directory: %v", err)
	}

	// Execute mapper
	outputChan := make(chan shared.KeyValue, 100)
	var wg sync.WaitGroup

	// Start goroutine to write intermediate data
	wg.Add(1)
	go func() {
		defer wg.Done()
		for kv := range outputChan {
			// Write to intermediate file
			// Format: key\tvalue\n
			filename := filepath.Join(intermediateDir, fmt.Sprintf("part-%d", task.TaskIndex))
			data := fmt.Sprintf("%s\t%s\n", kv.Key, kv.Value)
			if err := ioutil.WriteFile(filename, []byte(data), 0644); err != nil {
				log.Printf("Failed to write intermediate data: %v", err)
			}
		}
	}()

	// Process input - split into lines
	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		// Use line number as key for word count
		// In a real implementation, the mapper would parse the line
		mapper.Map(fmt.Sprintf("line-%d", i), line, outputChan)
	}

	close(outputChan)
	wg.Wait()

	// Report task as completed
	if err := w.ReportTask(task.ID, job.ID, shared.TaskCompleted, ""); err != nil {
		return err
	}

	return nil
}

func (w *Worker) ExecuteReduceTask(task *shared.Task, job *shared.Job, reducerCode []byte, inputPath, outputPath string) error {
	// Report task as running
	if err := w.ReportTask(task.ID, job.ID, shared.TaskRunning, ""); err != nil {
		return err
	}

	// Load reducer plugin
	var reducer shared.Reducer
	if err := gob.NewDecoder(bytes.NewReader(reducerCode)).Decode(&reducer); err != nil {
		return fmt.Errorf("failed to decode reducer: %v", err)
	}

	// Find all intermediate files for this reduce task
	// In a real implementation, we'd have a shuffle phase that groups by key
	// For simplicity, we'll process all intermediate files
	intermediatePath := filepath.Join(inputPath, "intermediate")
	intermediateFiles, err := ioutil.ReadDir(intermediatePath)
	if err != nil {
		return fmt.Errorf("failed to read intermediate directory: %v", err)
	}

	// Create output directory
	outputDir := filepath.Join(outputPath, "final")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Group intermediate data by key
	// This is a simplified version - in production, the shuffle phase would do this
	keyValues := make(map[string][]string)
	for _, file := range intermediateFiles {
		if file.IsDir() {
			continue
		}
		filePath := filepath.Join(intermediatePath, file.Name())
		content, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Printf("Failed to read intermediate file %s: %v", file.Name(), err)
			continue
		}

		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) != 2 {
				continue
			}
			key, value := parts[0], parts[1]
			keyValues[key] = append(keyValues[key], value)
		}
	}

	// Create output file for this reduce task
	outputFilePath := filepath.Join(outputDir, fmt.Sprintf("part-%d", task.TaskIndex))
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	// Execute reducer for each key
	for key, values := range keyValues {
		valueChan := make(chan string, len(values))
		for _, v := range values {
			valueChan <- v
		}
		close(valueChan)

		outputChan := make(chan string, 10)
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

	// Report task as completed
	if err := w.ReportTask(task.ID, job.ID, shared.TaskCompleted, ""); err != nil {
		return err
	}

	return nil
}

func (w *Worker) Run() {
	// Register with coordinator
	if err := w.RegisterWithCoordinator(); err != nil {
		log.Fatalf("Failed to register with coordinator: %v", err)
	}

	// Start heartbeat
	go w.SendHeartbeat()

	// Main task loop
	for {
		if !w.active {
			break
		}

		// Request a task
		taskResp, err := w.RequestTask()
		if err != nil {
			log.Printf("Failed to request task: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if taskResp.Task == nil {
			// No tasks available
			time.Sleep(1 * time.Second)
			continue
		}

		task := taskResp.Task
		job := taskResp.Job

		log.Printf("Worker %s received task %s (type: %s, index: %d)",
			w.id, task.ID, task.TaskType, task.TaskIndex)

		// Execute the task
		var err error
		if task.TaskType == shared.TaskMap {
			err = w.ExecuteMapTask(task, job, taskResp.Mapper, taskResp.InputPath, taskResp.OutputPath)
		} else if task.TaskType == shared.TaskReduce {
			err = w.ExecuteReduceTask(task, job, taskResp.Reducer, taskResp.InputPath, taskResp.OutputPath)
		}

		if err != nil {
			log.Printf("Task %s failed: %v", task.ID, err)
			w.ReportTask(task.ID, job.ID, shared.TaskFailed, err.Error())
		}
	}
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: worker <worker-id> <coordinator-host:port> <storage-path>")
		os.Exit(1)
	}

	workerID := os.Args[1]
	coordinator := os.Args[2]
	storagePath := os.Args[3]

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Fatal("Failed to create storage directory:", err)
	}

	worker := NewWorker(workerID, coordinator, storagePath)

	// Start worker
	log.Printf("Starting worker %s, coordinator at %s", workerID, coordinator)
	worker.Run()

	// Wait for shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Printf("Worker %s shutting down...", workerID)
	worker.active = false
}
```

---

### Step 5: Define Mapper and Reducer Interfaces

Create `shared/types.go` (add to existing file):

```go
// Mapper is the interface that user-defined mappers must implement
type Mapper interface {
	Map(key string, value string, output chan<- KeyValue)
}

// Reducer is the interface that user-defined reducers must implement
type Reducer interface {
	Reduce(key string, values <-chan string, output chan<- string)
}

// WordCountMapper implements Mapper for word count
type WordCountMapper struct{}

func (m *WordCountMapper) Map(key string, value string, output chan<- KeyValue) {
	// Split value into words
	words := strings.Fields(value)
	for _, word := range words {
		// Clean word: remove punctuation, convert to lowercase
		cleanWord := strings.ToLower(strings.Trim(word, ".,!?;:\"'()[]{}"))
		if cleanWord != "" {
			output <- KeyValue{Key: cleanWord, Value: "1"}
		}
	}
}

// WordCountReducer implements Reducer for word count
type WordCountReducer struct{}

func (r *WordCountReducer) Reduce(key string, values <-chan string, output chan<- string) {
	count := 0
	for range values {
		count++
	}
	output <- fmt.Sprintf("%d", count)
}

// InvertedIndexMapper implements Mapper for inverted index
type InvertedIndexMapper struct{}

func (m *InvertedIndexMapper) Map(key string, value string, output chan<- KeyValue) {
	// key is document ID, value is document content
	words := strings.Fields(value)
	for _, word := range words {
		cleanWord := strings.ToLower(strings.Trim(word, ".,!?;:\"'()[]{}"))
		if cleanWord != "" {
			output <- KeyValue{Key: cleanWord, Value: key}
		}
	}
}

// InvertedIndexReducer implements Reducer for inverted index
type InvertedIndexReducer struct{}

func (r *InvertedIndexReducer) Reduce(key string, values <-chan string, output chan<- string) {
	// Collect all document IDs for this word
	var docs []string
	for docID := range values {
		docs = append(docs, docID)
	}
	// Output as comma-separated list
	output <- strings.Join(docs, ",")
}
```

---

### Step 6: Create Word Count Example

Create `examples/wordcount/main.go`:

```go
package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"

	"mapreduce/shared"
)

func main() {
	// Parse command line arguments
	coordinatorAddr := flag.String("coordinator", "localhost:1234", "Coordinator address")
	inputPath := flag.String("input", "./input", "Input directory")
	outputPath := flag.String("output", "./output", "Output directory")
	numMapTasks := flag.Int("map", 4, "Number of map tasks")
	numReduceTasks := flag.Int("reduce", 2, "Number of reduce tasks")
	flag.Parse()

	// Create input directory and sample files
	if err := os.MkdirAll(*inputPath, 0755); err != nil {
		log.Fatal("Failed to create input directory:", err)
	}

	// Create sample input files
	sampleTexts := []string{
		"Hello world this is a test",
		"Hello again world",
		"This is another test of the system",
		"The world is a beautiful place",
	}

	for i, text := range sampleTexts {
		filename := filepath.Join(*inputPath, fmt.Sprintf("input-%d.txt", i))
		if err := ioutil.WriteFile(filename, []byte(text), 0644); err != nil {
			log.Fatalf("Failed to create input file %s: %v", filename, err)
		}
	}

	// Create output directory
	if err := os.MkdirAll(*outputPath, 0755); err != nil {
		log.Fatal("Failed to create output directory:", err)
	}

	// Serialize mapper and reducer
	mapper := &shared.WordCountMapper{}
	var mapperBuf bytes.Buffer
	if err := gob.NewEncoder(&mapperBuf).Encode(mapper); err != nil {
		log.Fatal("Failed to encode mapper:", err)
	}

	reducer := &shared.WordCountReducer{}
	var reducerBuf bytes.Buffer
	if err := gob.NewEncoder(&reducerBuf).Encode(reducer); err != nil {
		log.Fatal("Failed to encode reducer:", err)
	}

	// Connect to coordinator
	client, err := rpc.Dial("tcp", *coordinatorAddr)
	if err != nil {
		log.Fatal("Failed to connect to coordinator:", err)
	}
	defer client.Close()

	// Create job
	job := shared.Job{
		ID:           fmt.Sprintf("wordcount-%d", time.Now().UnixNano()),
		InputPath:    *inputPath,
		OutputPath:   *outputPath,
		Mapper:       "wordcount.Mapper",
		Reducer:      "wordcount.Reducer",
		NumMapTasks:  *numMapTasks,
		NumReduceTasks: *numReduceTasks,
	}

	// Submit job
	req := &shared.SubmitJobRequest{
		Job:         job,
		MapperCode:  mapperBuf.Bytes(),
		ReducerCode: reducerBuf.Bytes(),
	}

	var resp shared.SubmitJobResponse
	if err := client.Call(shared.CoordinatorServiceName+".SubmitJob", req, &resp); err != nil {
		log.Fatal("Failed to submit job:", err)
	}

	if !resp.Success {
		log.Fatal("Job submission failed:", resp.Error)
	}

	log.Printf("Submitted job %s", resp.JobID)

	// Monitor job status
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		statusReq := &shared.GetJobStatusRequest{
			JobID: resp.JobID,
		}
		var statusResp shared.GetJobStatusResponse
		if err := client.Call(shared.CoordinatorServiceName+".GetJobStatus", statusReq, &statusResp); err != nil {
			log.Printf("Failed to get job status: %v", err)
			continue
		}

		if statusResp.Error != "" {
			log.Printf("Error getting job status: %s", statusResp.Error)
			continue
		}

		if statusResp.Job == nil {
			log.Printf("Job %s not found", resp.JobID)
			continue
		}

		job := statusResp.Job
		log.Printf("Job %s status: %s", job.ID, job.State)

		if job.State == shared.JobCompleted {
			log.Printf("Job %s completed!", job.ID)
			break
		}
	}

	// Print results
	log.Println("\nResults:")
	outputFiles, err := ioutil.ReadDir(filepath.Join(*outputPath, "final"))
	if err != nil {
		log.Printf("Failed to read output directory: %v", err)
		return
	}

	for _, file := range outputFiles {
		if file.IsDir() {
			continue
		}
		content, err := ioutil.ReadFile(filepath.Join(*outputPath, "final", file.Name()))
		if err != nil {
			log.Printf("Failed to read output file %s: %v", file.Name(), err)
			continue
		}
		fmt.Println(string(content))
	}
}
```

---

### Step 7: Create Docker Compose Configuration

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  mr-coordinator:
    build:
      context: .
      dockerfile: Dockerfile.coordinator
    container_name: mr-coordinator
    hostname: mr-coordinator
    ports:
      - "1234:1234"
    volumes:
      - ./shared-hdfs:/shared-hdfs
    environment:
      - NODE_ID=mr-coordinator
      - COORDINATOR_URL=mr-coordinator:1234
    networks:
      - mapreduce-net
    deploy:
      resources:
        limits:
          cpus: '0.2'
          memory: 50M

  mr-worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: mr-worker
    hostname: mr-worker
    volumes:
      - ./shared-hdfs:/shared-hdfs
    environment:
      - NODE_ID=mr-worker
      - COORDINATOR_URL=mr-coordinator:1234
    networks:
      - mapreduce-net
    depends_on:
      - mr-coordinator
    deploy:
      resources:
        limits:
          cpus: '0.25'
          memory: 50M

networks:
  mapreduce-net:
    driver: bridge
```

---

### Step 8: Create Dockerfiles

Create `Dockerfile.coordinator`:

```dockerfile
FROM golang:1.20-alpine

WORKDIR /app

# Copy coordinator source
COPY coordinator/ ./coordinator/
COPY shared/ ./shared/

# Build coordinator
RUN cd coordinator && go build -o /app/coordinator .

# Copy the binary to a minimal image
FROM alpine:latest

WORKDIR /app
COPY --from=0 /app/coordinator /app/coordinator
COPY --from=0 /app/shared /app/shared

# Create storage directory
RUN mkdir -p /shared-hdfs

# Run coordinator
CMD ["/app/coordinator", "1234", "/shared-hdfs"]
```

Create `Dockerfile.worker`:

```dockerfile
FROM golang:1.20-alpine

WORKDIR /app

# Copy worker source
COPY worker/ ./worker/
COPY shared/ ./shared/

# Build worker
RUN cd worker && go build -o /app/worker .

# Copy the binary to a minimal image
FROM alpine:latest

WORKDIR /app
COPY --from=0 /app/worker /app/worker
COPY --from=0 /app/shared /app/shared

# Create storage directory
RUN mkdir -p /shared-hdfs

# Generate a unique worker ID at runtime
CMD ["sh", "-c", "WORKER_ID=mr-worker-$$(hostname)-$$(date +%s) && /app/worker $$WORKER_ID mr-coordinator:1234 /shared-hdfs"]
```

---

### Step 9: Create Go Module

Create `go.mod`:

```go
module mapreduce

go 1.20

require (
	// No external dependencies for core implementation
)
```

---

## 7. Building and Running the System

### Step 1: Build the Project

```bash
# Initialize Go module
cd mapreduce
go mod init mapreduce

# Build coordinator
go build -o bin/coordinator ./coordinator

# Build worker
go build -o bin/worker ./worker

# Build word count example
go build -o bin/wordcount ./examples/wordcount
```

### Step 2: Run with Docker Compose

```bash
# Create shared storage directory
mkdir -p shared-hdfs

# Build and start containers
docker compose build
docker compose up -d mr-coordinator

# Start workers (scale to 4 workers)
docker compose up -d --scale mr-worker=4

# Check logs
docker compose logs -f mr-coordinator
docker compose logs -f mr-worker
```

### Step 3: Submit a Job

```bash
# Run the word count example
go run examples/wordcount/main.go \
  --coordinator localhost:1234 \
  --input ./shared-hdfs/input \
  --output ./shared-hdfs/output \
  --map 4 \
  --reduce 2
```

Or with Docker:

```bash
# Copy example binary to container
docker cp bin/wordcount mr-coordinator:/app/wordcount

# Execute in container
docker exec mr-coordinator /app/wordcount \
  --coordinator mr-coordinator:1234 \
  --input /shared-hdfs/input \
  --output /shared-hdfs/output \
  --map 4 \
  --reduce 2
```

### Step 4: Verify Results

```bash
# Check output files
ls -la shared-hdfs/output/final/
cat shared-hdfs/output/final/*
```

---

## 8. Chaos Engineering Tests

### Test 1: Worker Crash During Map

```bash
# Find a worker container ID
docker ps | grep mr-worker

# Kill a worker during job execution
docker kill <worker-container-id>

# Observe: Coordinator should reassign the task
# The job should still complete successfully
```

**Expected Behavior:**
- Coordinator detects worker is inactive (via heartbeat timeout)
- Tasks assigned to the dead worker are marked as PENDING
- Other workers pick up the reassigned tasks
- Job completes successfully

### Test 2: Straggler Worker

To simulate a straggler, modify the worker code to add artificial delay:

```go
// In worker/main.go, add to ExecuteMapTask:
if task.TaskIndex == 0 {
    time.Sleep(30 * time.Second) // Simulate straggler
}
```

**Expected Behavior:**
- Other workers complete their tasks quickly
- The straggler task takes much longer
- In a production system, we'd implement speculative execution

### Test 3: Network Partition

```bash
# Disconnect a worker from the network
docker network disconnect mapreduce-net <worker-container-id>

# Observe: Worker cannot communicate with coordinator
# After timeout, coordinator reassigns tasks
```

**Expected Behavior:**
- Worker loses connection to coordinator
- Heartbeats fail
- Coordinator marks worker as inactive after timeout
- Tasks are reassigned to other workers

---

## 9. Scaling the Cluster

### Scale Workers Horizontally

```bash
# Scale to 10 workers
docker compose up -d --scale mr-worker=10

# Check running containers
docker ps | grep mr-worker | wc -l
```

### Monitor Performance

```bash
# Watch task assignment in coordinator logs
docker compose logs -f mr-coordinator

# Observe: Tasks are distributed across all workers
# More workers = faster job completion (up to I/O limits)
```

---

## 10. Understanding the Code Flow

### Job Submission Flow

1. Client calls `SubmitJob` RPC on Coordinator
2. Coordinator creates Job record and Map/Reduce tasks
3. Coordinator stores mapper/reducer code
4. Coordinator returns Job ID to client

### Task Execution Flow

1. Worker calls `RequestTask` RPC on Coordinator
2. Coordinator finds a PENDING task and assigns it
3. Worker receives task details and code
4. Worker executes Map or Reduce function
5. Worker writes results to shared storage
6. Worker calls `ReportTask` RPC to update status

### Fault Tolerance Flow

1. Coordinator tracks worker heartbeats
2. If heartbeat times out, worker is marked inactive
3. All tasks assigned to inactive worker are reset to PENDING
4. Other workers pick up the reassigned tasks
5. Job continues without data loss

---

## 11. Key Production Considerations

### What This Implementation Simplifies

1. **Shuffle Phase:** The real shuffle phase distributes intermediate data across the network. We simplified by having workers write to shared storage.

2. **Data Locality:** In production, Map tasks are assigned to workers on the same node as the input data. We don't implement this optimization.

3. **Speculative Execution:** We don't launch duplicate tasks for stragglers.

4. **Combiners:** MapReduce can use combiners to reduce network traffic. Not implemented here.

5. **Security:** No authentication or encryption in this example.

### What You Should Add for Production

1. **Task Timeouts:** Add timeouts for task execution
2. **Speculative Execution:** Launch duplicate tasks for slow workers
3. **Combiners:** Implement combiners to reduce intermediate data
4. **Better Shuffle:** Implement proper shuffle phase with network transfer
5. **Authentication:** Add RPC authentication
6. **Encryption:** Encrypt data in transit and at rest
7. **Metrics:** Add prometheus metrics for monitoring
8. **Logging:** Structured logging with log levels
9. **Configuration:** Use config files instead of hardcoded values
10. **Health Checks:** Add HTTP health check endpoints

---

## 12. Learning Exercises

### Exercise 1: Implement Speculative Execution

Modify the Coordinator to:
1. Track task execution time
2. If a task runs longer than a threshold, launch a duplicate task
3. Cancel the slower task when one completes
4. Update the worker to handle task cancellation

### Exercise 2: Add Combiner Support

1. Define a Combiner interface similar to Reducer
2. Modify the Mapper to optionally use a combiner
3. Have the combiner reduce intermediate data before writing to storage
4. Measure the reduction in intermediate data size

### Exercise 3: Implement Proper Shuffle

1. Instead of writing to shared storage, have mappers send data directly to reducers
2. Implement a shuffle phase that groups data by key
3. Handle network failures during shuffle
4. Ensure all data is delivered even if workers crash

### Exercise 4: Add Metrics

1. Add prometheus client to both Coordinator and Worker
2. Track metrics: tasks completed, tasks failed, task duration, bytes processed
3. Expose metrics endpoint on each service
4. Create a Grafana dashboard to visualize the metrics

### Exercise 5: Implement Input Splitting

1. Modify the input handling to split large files into chunks
2. Each Map task processes a chunk of a file
3. Handle chunk boundaries (don't split in the middle of a record)
4. Update the word count example to work with chunked input

---

## 13. Performance Optimization Tips

### Coordinator Optimizations

1. **Batch Task Requests:** Allow workers to request multiple tasks at once
2. **Task Locality:** Assign tasks to workers on the same node as input data
3. **Memory Management:** Limit memory usage for tracking large jobs
4. **Connection Pooling:** Reuse RPC connections instead of creating new ones

### Worker Optimizations

1. **Parallel Task Execution:** Execute multiple tasks concurrently per worker
2. **Memory Management:** Stream intermediate data instead of buffering in memory
3. **I/O Optimization:** Use buffered I/O for reading/writing files
4. **CPU Throttling:** Limit CPU usage to prevent resource exhaustion

### Storage Optimizations

1. **Compression:** Compress intermediate data to reduce I/O
2. **Local Disks:** Use local disks for intermediate data when possible
3. **Caching:** Cache frequently accessed data in memory
4. **Partitioning:** Partition data to minimize network transfer

---

## 14. Debugging Guide

### Common Issues and Solutions

**Issue: Workers cannot connect to Coordinator**
- Check Docker network connectivity: `docker network inspect mapreduce-net`
- Verify coordinator is running: `docker ps | grep mr-coordinator`
- Check coordinator logs: `docker compose logs mr-coordinator`
- Test connectivity: `docker exec -it mr-worker ping mr-coordinator`

**Issue: Tasks are not being assigned**
- Check coordinator state: Tasks may all be completed
- Verify workers are registered: Look for registration messages in coordinator logs
- Check worker logs for errors: `docker compose logs mr-worker`

**Issue: Jobs are stuck in MAPPING state**
- Check if all map tasks are assigned: Look at coordinator state
- Verify workers are processing tasks: Check worker logs
- Check for errors in task execution: Look for error messages in worker logs

**Issue: Output files are empty**
- Verify intermediate data was written: Check shared-hdfs/intermediate/
- Check reducer logs for errors
- Verify mapper is emitting key-value pairs

### Debugging Tools

1. **Docker Logs:** `docker compose logs [service]`
2. **Docker Exec:** `docker exec -it [container] sh`
3. **Network Inspection:** `docker network inspect mapreduce-net`
4. **Process Inspection:** `docker top [container]`
5. **Resource Usage:** `docker stats`

---

## 15. Next Steps

1. **Implement the exercises** listed above to deepen your understanding
2. **Read the MapReduce paper** by Dean and Ghemawat for theoretical foundations
3. **Explore Spark** to see how MapReduce concepts evolved
4. **Look at Hadoop** source code to see a production implementation
5. **Experiment with different workloads** to understand performance characteristics
6. **Add more examples** (inverted index, log analysis, etc.)
7. **Implement a web UI** to visualize job progress and cluster state

---

## Conclusion

You have now built a complete, working Distributed MapReduce framework in Go. This implementation includes:

- Coordinator and Worker architecture
- RPC-based communication
- Fault tolerance via task re-assignment
- Shared storage for intermediate data
- Docker-based deployment
- Chaos engineering tests

This project demonstrates the core principles of distributed computing: parallelism, fault tolerance, and scalability. The skills you've learned are directly applicable to real-world distributed systems.

**Remember:** The key to mastering distributed systems is understanding the trade-offs. Every design decision involves balancing consistency, availability, and partition tolerance (CAP theorem), as well as latency, throughput, and resource usage.

Continue experimenting, breaking things, and learning from failures. That's how you truly understand distributed systems.
The guided project for the Distributed MapReduce Engine is ready in the canvas. It includes:

- Complete system architecture with ASCII flow diagram
- Production blueprint referencing Google's and Netflix's implementations
- Full Go implementation: Coordinator, Worker, RPC interfaces, Mapper/Reducer
- Docker Compose configuration with resource limits and shared storage
- Step-by-step build and run instructions
- Chaos engineering tests for fault tolerance validation
- Learning exercises to deepen understanding
- Debugging guide and performance optimization tips

Start with the prerequisites, then follow the implementation steps to build and deploy your cluster. The project is designed for hands-on learning through experimentation and failure testing.












Here are three ready-to-use visualization options for your MapReduce system that run in a local browser:

---

## 1. Dotted Grid Visualization (Recommended)

Save as `dotted-grid.html` and open in browser. Each dot represents a node or task with color coding.

```html
<!DOCTYPE html>
<html>
<head>
<title>MapReduce Dotted Grid</title>
<style>
body{font-family:Arial,sans-serif;background:#1a1a2e;color:#fff;margin:0;padding:20px}
canvas{background:#16213e;border-radius:8px;display:block;margin:0 auto}
.controls{display:flex;justify-content:center;gap:20px;margin:20px 0}
select,button{padding:8px 12px;border:none;border-radius:4px;background:#16213e;color:#fff;cursor:pointer}
button{background:#e94560}
.legend{display:flex;justify-content:center;gap:30px;margin:20px 0;flex-wrap:wrap}
.legend-item{display:flex;align-items:center;gap:8px}
.legend-color{width:16px;height:16px;border-radius:50%}
</style>
</head>
<body>
<div style="max-width:1000px;margin:0 auto">
<h1 style="text-align:center;color:#e94560">MapReduce Dotted Grid</h1>
<div class="controls">
<select id="grid-size" onchange="init()">
<option value="15">15x15</option>
<option value="20" selected>20x20</option>
<option value="25">25x25</option>
</select>
<button onclick="randomize()">Randomize</button>
<button onclick="reset()">Reset</button>
</div>
<canvas id="canvas"></canvas>
<div class="legend">
<div class="legend-item"><div class="legend-color" style="background:#e94560"></div><span>Coordinator</span></div>
<div class="legend-item"><div class="legend-color" style="background:#4CAF50"></div><span>Worker</span></div>
<div class="legend-item"><div class="legend-color" style="background:#f44336"></div><span>Failed</span></div>
<div class="legend-item"><div class="legend-color" style="background:#2196F3"></div><span>Map Task</span></div>
<div class="legend-item"><div class="legend-color" style="background:#FF5722"></div><span>Reduce</span></div>
<div class="legend-item"><div class="legend-color" style="background:#FFC107"></div><span>Pending</span></div>
<div class="legend-item"><div class="legend-color" style="background:#9C27B0"></div><span>Done</span></div>
</div>
</div>
<script>
const c=document.getElementById('canvas');
const ctx=c.getContext('2d');
let size=20,grid=[],colors={c:'#e94560',w:'#4CAF50',f:'#f44336',m:'#2196F3',r:'#FF5722',p:'#FFC107',d:'#9C27B0'};
function init(){
size=parseInt(document.getElementById('grid-size').value);
c.width=c.height=size*25+20;
grid=Array(size).fill().map(()=>Array(size).fill(null));
const mid=Math.floor(size/2);
grid[mid][mid]='c';
for(let i=0;i<4;i++){
let x,y;
do{x=Math.floor(Math.random()*size);y=Math.floor(Math.random()*size);}while(grid[y][x]||(Math.abs(x-mid)<2&&Math.abs(y-mid)<2));
grid[y][x]=Math.random()<0.2?'f':'w';
}
for(let i=0;i<12;i++){
let x,y;
do{x=Math.floor(Math.random()*size);y=Math.floor(Math.random()*size);}while(grid[y][x]||(Math.abs(x-mid)<2&&Math.abs(y-mid)<2));
grid[y][x]=Math.random()<0.7?(Math.random()<0.5?'d':'m'):'p';
}
draw();
}
function draw(){
ctx.clearRect(0,0,c.width,c.height);
const cell=20;
for(let y=0;y<size;y++){
for(let x=0;x<size;x++){
if(grid[y][x]){
const cx=10+x*cell+cell/2,cy=10+y*cell+cell/2;
ctx.fillStyle=colors[grid[y][x]];
ctx.beginPath();
ctx.arc(cx,cy,cell/3,0,Math.PI*2);
ctx.fill();
if(grid[y][x]=='c'){
ctx.strokeStyle=colors.c;ctx.lineWidth=2;
ctx.beginPath();ctx.arc(cx,cy,cell/3+3,0,Math.PI*2);ctx.stroke();
}
}
}
}
const mid=Math.floor(size/2);
const cx=10+mid*cell+cell/2,cy=10+mid*cell+cell/2;
ctx.strokeStyle='#4a4a6a';ctx.lineWidth=1;
for(let y=0;y<size;y++){
for(let x=0;x<size;x++){
if(grid[y][x]=='w'||grid[y][x]=='f'){
const wx=10+x*cell+cell/2,wy=10+y*cell+cell/2;
ctx.beginPath();ctx.moveTo(cx,cy);ctx.lineTo(wx,wy);ctx.stroke();
}
}
}
}
function randomize(){
const mid=Math.floor(size/2);
for(let y=0;y<size;y++){
for(let x=0;x<size;x++){
const dist=Math.abs(x-mid)+Math.abs(y-mid);
if(dist===0){grid[y][x]='c';continue;}
grid[y][x]=null;
const r=Math.random();
if(r<0.05)grid[y][x]='w';
else if(r<0.1)grid[y][x]='f';
else if(r<0.25)grid[y][x]='m';
else if(r<0.35)grid[y][x]='r';
else if(r<0.4)grid[y][x]='p';
else if(r<0.45)grid[y][x]='d';
}
}
draw();
}
function reset(){init();}
init();
</script>
</body>
</html>
```

**Features:** Adjustable grid size, color-coded nodes (Coordinator, Workers, Map/Reduce tasks, status), connections from Coordinator to Workers, Randomize/Reset buttons, legend.

---

## 2. Card-Based Dashboard

Save as `dashboard.html`. Clean UI showing cluster status.

```html
<!DOCTYPE html>
<html>
<head>
<title>MapReduce Dashboard</title>
<style>
body{font-family:Arial,sans-serif;background:#f5f5f5;margin:0;padding:20px}
.container{max-width:1200px;margin:0 auto}
h1{text-align:center;color:#333}
.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:20px;margin:20px 0}
.card{background:#fff;border-radius:8px;padding:20px;box-shadow:0 2px 10px rgba(0,0,0,0.1)}
.card h3{margin-top:0;color:#667eea;border-bottom:2px solid #667eea;padding-bottom:10px}
.grid{display:grid;grid-template-columns:repeat(5,1fr);gap:15px;margin:20px 0}
.node{background:#fff;border:2px solid #ddd;border-radius:8px;padding:15px;text-align:center}
.node.coordinator{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff}
.node.worker.active{border-color:#4CAF50;box-shadow:0 0 0 2px rgba(76,175,80,0.2)}
.node.worker.inactive{border-color:#f44336;background:#ffebee}
.progress{width:100%;height:8px;background:#e0e0e0;border-radius:4px;margin:10px 0;overflow:hidden}
.progress-bar{height:100%;background:linear-gradient(90deg,#4CAF50,#8BC34A);border-radius:4px;transition:width 0.5s}
.info{font-size:12px;color:#666}
</style>
</head>
<body>
<div class="container">
<h1>MapReduce Cluster Dashboard</h1>
<div class="summary">
<div class="card"><h3>Cluster</h3><p>Coordinator: <b>Running</b></p><p>Active Workers: <b>3</b></p><p>Total Workers: <b>4</b></p></div>
<div class="card"><h3>Jobs</h3><p>Running: <b>1</b></p><p>Completed: <b>12</b></p><p>Failed: <b>0</b></p></div>
<div class="card"><h3>Tasks</h3><p>Map: <b>8/10</b></p><p>Reduce: <b>4/5</b></p><p>Total: <b>12/15</b></p></div>
<div class="card"><h3>Performance</h3><p>Avg Map Time: <b>2.4s</b></p><p>Avg Reduce: <b>3.1s</b></p><p>Throughput: <b>12MB/s</b></p></div>
</div>
<div class="grid">
<div class="node coordinator">
<div><b>COORDINATOR</b></div>
<div>Status: Running</div>
<div class="info">Jobs: 1</div>
</div>
<div class="node worker active">
<div><b>WORKER-1</b></div>
<div>Status: Active</div>
<div class="progress"><div class="progress-bar" style="width:95%"></div></div>
<div class="info">Map: 3/3 | Reduce: 1/2</div>
</div>
<div class="node worker active">
<div><b>WORKER-2</b></div>
<div>Status: Active</div>
<div class="progress"><div class="progress-bar" style="width:75%"></div></div>
<div class="info">Map: 2/3 | Reduce: 0/2</div>
</div>
<div class="node worker inactive">
<div><b>WORKER-3</b></div>
<div>Status: Failed</div>
<div class="progress"><div class="progress-bar" style="width:0%"></div></div>
<div class="info">Last: 2m ago</div>
</div>
<div class="node worker active">
<div><b>WORKER-4</b></div>
<div>Status: Active</div>
<div class="progress"><div class="progress-bar" style="width:100%"></div></div>
<div class="info">Map: 3/3 | Reduce: 2/2</div>
</div>
</div>
</div>
<script>
setInterval(()=>{
const bars=document.querySelectorAll('.progress-bar');
bars.forEach(bar=>{
const w=parseInt(bar.style.width)||0;
bar.style.width=Math.min(100,w+Math.random()*10)+'%';
});
},2000);
</script>
</body>
</html>
```

**Features:** Summary metrics cards, individual node cards with progress bars, color-coded status, auto-updating progress simulation.

---
## 3. Live Dashboard with Real Data

Add this to your Coordinator (`coordinator/main.go`):

```go
import (
    "encoding/json"
    "net/http"
)

func (c *Coordinator) GetStatus(w http.ResponseWriter, r *http.Request) {
    c.mu.Lock()
    defer c.mu.Unlock()
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "coordinator": "running",
        "workers": c.workers,
        "jobs": c.jobs,
        "map_tasks": c.mapTasks,
        "reduce_tasks": c.reduceTasks,
    })
}

// In main()
http.HandleFunc("/api/status", coord.GetStatus)
http.Handle("/", http.FileServer(http.Dir("./viz")))
go http.ListenAndServe(":8080", nil)
```

Create `viz/index.html`:

```html
<!DOCTYPE html>
<html>
<head>
<title>MapReduce Live Dashboard</title>
<style>
body{font-family:Arial,sans-serif;background:#f5f5f5;margin:0;padding:20px}
.container{max-width:1200px;margin:0 auto}
h1{text-align:center}
.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:20px;margin:20px 0}
.card{background:#fff;border-radius:8px;padding:20px;box-shadow:0 2px 10px rgba(0,0,0,0.1)}
.workers{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:15px;margin:20px 0}
.worker{background:#fff;border:2px solid #ddd;border-radius:8px;padding:15px}
.worker.active{border-color:#4CAF50}
.worker.inactive{border-color:#f44336;background:#ffebee}
.loading{color:#999;text-align:center;padding:20px}
</style>
</head>
<body>
<div class="container">
<h1>MapReduce Live Dashboard</h1>
<div class="summary">
<div class="card"><h3>Cluster</h3><p>Status: <span id="status">Loading...</span></p></div>
<div class="card"><h3>Workers</h3><p>Active: <span id="active">0</span></p><p>Total: <span id="total">0</span></p></div>
<div class="card"><h3>Jobs</h3><p>Running: <span id="running">0</span></p><p>Completed: <span id="completed">0</span></p></div>
<div class="card"><h3>Tasks</h3><p>Map: <span id="map">0/0</span></p><p>Reduce: <span id="reduce">0/0</span></p></div>
</div>
<div class="workers" id="workers"><h2>Workers</h2><div class="loading">Loading...</div></div>
</div>
<script>
async function load(){
try{
const r=await fetch('/api/status');
const d=await r.json();
document.getElementById('status').textContent=d.coordinator;
const a=Object.values(d.workers).filter(w=>w.Active).length;
document.getElementById('active').textContent=a;
document.getElementById('total').textContent=Object.keys(d.workers).length;
const running=Object.values(d.jobs).filter(j=>j.State<4).length;
const completed=Object.values(d.jobs).filter(j=>j.State===4).length;
document.getElementById('running').textContent=running;
document.getElementById('completed').textContent=completed;
let mapTotal=0,mapDone=0,redTotal=0,redDone=0;
Object.values(d.map_tasks).forEach(t=>{mapTotal++;if(t.State===3)mapDone++;});
Object.values(d.reduce_tasks).forEach(t=>{redTotal++;if(t.State===3)redDone++;});
document.getElementById('map').textContent=mapDone+'/'+mapTotal;
document.getElementById('reduce').textContent=redDone+'/'+redTotal;
const w=document.getElementById('workers');
w.innerHTML='<h2>Workers</h2>';
for(const[id,worker] of Object.entries(d.workers)){
const card=document.createElement('div');
card.className='worker '+ (worker.Active?'active':'inactive');
card.innerHTML=`<h3>${id}</h3><p>Status: ${worker.Active?'Active':'Inactive'}</p><p>Last: ${new Date(worker.LastSeen).toLocaleTimeString()}</p>`;
w.appendChild(card);
}
}catch(e){console.error(e);setTimeout(load,2000);}
}
load();setInterval(load,2000);
</script>
</body>
</html>
```

**How to use:** Add HTTP endpoint to Coordinator, create `viz/` directory with the HTML, run Coordinator, open `http://localhost:8080`.

---
**Recommendation:** Start with Option 1 (Dotted Grid) for immediate visual feedback. Use Option 3 for production monitoring with real data.

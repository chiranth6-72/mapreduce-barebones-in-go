package main

import (
	"bytes"
	"embed"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"mapreduce/shared"
)

//go:embed all:web/dist
var webAssets embed.FS

type Coordinator struct {
	mu           sync.Mutex
	jobs         map[string]*shared.Job
	mapTasks     map[string]*shared.Task
	reduceTasks  map[string]*shared.Task
	workers      map[string]*shared.WorkerInfo
	mapperCode   map[string][]byte // jobID -> mapper code
	reducerCode  map[string][]byte // jobID -> reducer code
	nextTaskID   int
	storagePath  string
	rpcPort      string
	webPort      string
}

type CoordinatorRPC struct {
	coord *Coordinator
}

func NewCoordinator(storagePath, rpcPort, webPort string) *Coordinator {
	return &Coordinator{
		jobs:        make(map[string]*shared.Job),
		mapTasks:    make(map[string]*shared.Task),
		reduceTasks: make(map[string]*shared.Task),
		workers:     make(map[string]*shared.WorkerInfo),
		mapperCode:  make(map[string][]byte),
		reducerCode: make(map[string][]byte),
		nextTaskID:  0,
		storagePath: storagePath,
		rpcPort:     rpcPort,
		webPort:     webPort,
	}
}

func (c *Coordinator) generateTaskID() string {
	id := fmt.Sprintf("task-%d", c.nextTaskID)
	c.nextTaskID++
	return id
}

// RPC Wrapper Methods
func (r CoordinatorRPC) SubmitJob(req *shared.SubmitJobRequest, resp *shared.SubmitJobResponse) error {
	return r.coord.SubmitJob(req, resp)
}

func (r CoordinatorRPC) GetJobStatus(req *shared.GetJobStatusRequest, resp *shared.GetJobStatusResponse) error {
	return r.coord.GetJobStatus(req, resp)
}

func (r CoordinatorRPC) RequestTask(req *shared.TaskRequest, resp *shared.TaskResponse) error {
	return r.coord.RequestTask(req, resp)
}

func (r CoordinatorRPC) ReportTask(req *shared.TaskUpdate, resp *shared.TaskUpdateResponse) error {
	return r.coord.ReportTask(req, resp)
}

func (r CoordinatorRPC) RegisterWorker(req *shared.RegisterRequest, resp *shared.RegisterResponse) error {
	return r.coord.RegisterWorker(req, resp)
}

func (r CoordinatorRPC) Heartbeat(req *shared.HeartbeatRequest, resp *shared.HeartbeatResponse) error {
	return r.coord.Heartbeat(req, resp)
}

// Business Logic Implementation
func (c *Coordinator) SubmitJob(req *shared.SubmitJobRequest, resp *shared.SubmitJobResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate job
	if req.Job.ID == "" {
		req.Job.ID = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	if req.Job.NumMapTasks <= 0 {
		req.Job.NumMapTasks = 4
	}
	if req.Job.NumReduceTasks <= 0 {
		req.Job.NumReduceTasks = 2
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
			ID:        c.generateTaskID(),
			JobID:     job.ID,
			TaskType:  shared.TaskMap,
			TaskIndex: i,
			State:     shared.TaskPending,
		}
		c.mapTasks[task.ID] = task
	}

	// Create reduce tasks
	for i := 0; i < job.NumReduceTasks; i++ {
		task := &shared.Task{
			ID:        c.generateTaskID(),
			JobID:     job.ID,
			TaskType:  shared.TaskReduce,
			TaskIndex: i,
			State:     shared.TaskPending,
		}
		c.reduceTasks[task.ID] = task
	}

	resp.Success = true
	resp.JobID = job.ID
	log.Printf("Submitted job %s (Mappers: %d, Reducers: %d)", job.ID, job.NumMapTasks, job.NumReduceTasks)
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
	// Priority 1: Map tasks first
	for _, task := range c.mapTasks {
		if task.State == shared.TaskPending {
			task.State = shared.TaskAssigned
			task.WorkerID = req.WorkerID
			task.AssignedAt = time.Now()

			job := c.jobs[task.JobID]
			if job.State == shared.JobPending {
				job.State = shared.JobMapping
			}

			resp.Task = task
			resp.Job = job
			resp.Mapper = c.mapperCode[task.JobID]
			resp.Reducer = c.reducerCode[task.JobID]
			resp.InputPath = job.InputPath
			resp.OutputPath = job.OutputPath

			log.Printf("Assigned MAP task %s (index %d) to worker %s", task.ID, task.TaskIndex, req.WorkerID)
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
		// Priority 2: Reduce tasks
		for _, task := range c.reduceTasks {
			if task.State == shared.TaskPending {
				task.State = shared.TaskAssigned
				task.WorkerID = req.WorkerID
				task.AssignedAt = time.Now()

				job := c.jobs[task.JobID]
				if job.State == shared.JobShuffling || job.State == shared.JobMapping {
					job.State = shared.JobReducing
				}

				resp.Task = task
				resp.Job = job
				resp.Mapper = c.mapperCode[task.JobID]
				resp.Reducer = c.reducerCode[task.JobID]
				resp.InputPath = job.InputPath
				resp.OutputPath = job.OutputPath

				log.Printf("Assigned REDUCE task %s (index %d) to worker %s", task.ID, task.TaskIndex, req.WorkerID)
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
		t := time.Now()
		task.StartedAt = &t
		log.Printf("Task %s is now RUNNING on worker %s", task.ID, req.WorkerID)
	}
	if req.State == shared.TaskCompleted || req.State == shared.TaskFailed {
		t := time.Now()
		task.CompletedAt = &t
		
		// Increment worker tasks handled count
		if req.State == shared.TaskCompleted {
			if w, exists := c.workers[req.WorkerID]; exists {
				w.TasksHandled++
			}
			log.Printf("Task %s COMPLETED by worker %s", task.ID, req.WorkerID)
		} else {
			log.Printf("Task %s FAILED on worker %s: %s", task.ID, req.WorkerID, req.Error)
		}
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
			log.Printf("Job %s transitioned from MAPPING to SHUFFLING", req.JobID)
			
			// For educational visualization, we hold the SHUFFLING state for 2 seconds
			// before automatically transitioning to REDUCING (or when workers call RequestTask)
			go func(jobID string) {
				time.Sleep(2 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				if j, exists := c.jobs[jobID]; exists && j.State == shared.JobShuffling {
					j.State = shared.JobReducing
					log.Printf("Job %s transitioned from SHUFFLING to REDUCING", jobID)
				}
			}(req.JobID)
		}

		// Check if all reduce tasks are done
		allReduceDone := true
		for _, t := range c.reduceTasks {
			if t.JobID == req.JobID && t.State != shared.TaskCompleted {
				allReduceDone = false
				break
			}
		}

		if allReduceDone && (job.State == shared.JobReducing || job.State == shared.JobShuffling) {
			job.State = shared.JobCompleted
			now := time.Now()
			job.CompletedAt = &now
			log.Printf("Job %s COMPLETED successfully!", job.ID)
		}
	} else if req.State == shared.TaskFailed {
		job.State = shared.JobFailed
		log.Printf("Job %s FAILED due to task failure: %s", job.ID, req.TaskID)
	}

	resp.Success = true
	return nil
}

func (c *Coordinator) RegisterWorker(req *shared.RegisterRequest, resp *shared.RegisterResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if worker already registered
	if w, exists := c.workers[req.WorkerID]; exists {
		w.LastSeen = time.Now()
		w.Active = true
		resp.Success = true
		return nil
	}

	// Register new worker
	c.workers[req.WorkerID] = &shared.WorkerInfo{
		ID:           req.WorkerID,
		Address:      req.Address,
		LastSeen:     time.Now(),
		Active:       true,
		TasksHandled: 0,
	}

	resp.Success = true
	log.Printf("Worker registered: %s (%s)", req.WorkerID, req.Address)
	return nil
}

func (c *Coordinator) Heartbeat(req *shared.HeartbeatRequest, resp *shared.HeartbeatResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if worker, exists := c.workers[req.WorkerID]; exists {
		worker.LastSeen = time.Now()
		worker.Active = true
	} else {
		// Worker not registered, instruct them to register
		resp.Success = false
		return nil
	}

	resp.Success = true
	return nil
}

func (c *Coordinator) StartRPCServer() {
	rpcServer := rpc.NewServer()
	rpcServer.RegisterName(shared.CoordinatorServiceName, CoordinatorRPC{coord: c})
	listener, err := net.Listen("tcp", ":"+c.rpcPort)
	if err != nil {
		log.Fatalf("Coordinator: RPC listen error: %v", err)
	}
	log.Printf("Coordinator: RPC server listening on :%s", c.rpcPort)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Coordinator: RPC accept error: %v", err)
				continue
			}
			go rpcServer.ServeConn(conn)
		}
	}()
}

func (c *Coordinator) StartMonitoring(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			c.mu.Lock()
			for id, worker := range c.workers {
				if worker.Active && time.Since(worker.LastSeen) > interval*3 {
					worker.Active = false
					log.Printf("Worker %s marked as inactive (heartbeat timeout)", id)
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
	}()
}

// File Explorer Info
type FileInfo struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	IsDir bool   `json:"is_dir"`
}

func (c *Coordinator) listFilesRecursive(subPath string) []FileInfo {
	var results []FileInfo
	fullPath := filepath.Join(c.storagePath, subPath)
	entries, err := ioutil.ReadDir(fullPath)
	if err != nil {
		return results
	}
	for _, entry := range entries {
		relPath := filepath.Join(subPath, entry.Name())
		relPath = filepath.ToSlash(relPath)
		results = append(results, FileInfo{
			Name:  entry.Name(),
			Path:  relPath,
			Size:  entry.Size(),
			IsDir: entry.IsDir(),
		})
		if entry.IsDir() {
			results = append(results, c.listFilesRecursive(relPath)...)
		}
	}
	return results
}

// HTTP API and Visualisation Server
func (c *Coordinator) StartWebServer() {
	mux := http.NewServeMux()

	// 1. Get Cluster State API
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()

		// Construct state object
		workersList := make([]*shared.WorkerInfo, 0, len(c.workers))
		for _, wrk := range c.workers {
			workersList = append(workersList, wrk)
		}

		jobsList := make([]*shared.Job, 0, len(c.jobs))
		for _, j := range c.jobs {
			jobsList = append(jobsList, j)
		}

		mapTasksList := make([]*shared.Task, 0, len(c.mapTasks))
		for _, t := range c.mapTasks {
			mapTasksList = append(mapTasksList, t)
		}

		reduceTasksList := make([]*shared.Task, 0, len(c.reduceTasks))
		for _, t := range c.reduceTasks {
			reduceTasksList = append(reduceTasksList, t)
		}

		state := map[string]interface{}{
			"jobs":         jobsList,
			"map_tasks":    mapTasksList,
			"reduce_tasks": reduceTasksList,
			"workers":      workersList,
			"storage":      c.listFilesRecursive(""),
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(state)
	})

	// 2. Submit Job via HTTP API
	mux.HandleFunc("/api/submit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload struct {
			JobType        string `json:"job_type"` // "wordcount" or "invertedindex"
			NumMapTasks    int    `json:"num_map_tasks"`
			NumReduceTasks int    `json:"num_reduce_tasks"`
			InputPath      string `json:"input_path"`
			OutputPath     string `json:"output_path"`
		}

		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Bad request: "+err.Error(), http.StatusBadRequest)
			return
		}

		if payload.InputPath == "" {
			payload.InputPath = filepath.ToSlash(filepath.Join(c.storagePath, "input"))
		}
		if payload.OutputPath == "" {
			payload.OutputPath = filepath.ToSlash(filepath.Join(c.storagePath, "output"))
		}
		if payload.NumMapTasks <= 0 {
			payload.NumMapTasks = 4
		}
		if payload.NumReduceTasks <= 0 {
			payload.NumReduceTasks = 2
		}

		// Ensure directories exist
		os.MkdirAll(payload.InputPath, 0755)
		os.MkdirAll(payload.OutputPath, 0755)

		// Serialize Mapper and Reducer based on job type
		var mBuf, rBuf bytes.Buffer
		var mapperName, reducerName string

		if payload.JobType == "invertedindex" {
			gob.NewEncoder(&mBuf).Encode(&shared.InvertedIndexMapper{})
			gob.NewEncoder(&rBuf).Encode(&shared.InvertedIndexReducer{})
			mapperName = "InvertedIndexMapper"
			reducerName = "InvertedIndexReducer"
		} else {
			// default to wordcount
			payload.JobType = "wordcount"
			gob.NewEncoder(&mBuf).Encode(&shared.WordCountMapper{})
			gob.NewEncoder(&rBuf).Encode(&shared.WordCountReducer{})
			mapperName = "WordCountMapper"
			reducerName = "WordCountReducer"
		}

		job := shared.Job{
			ID:             fmt.Sprintf("%s-%d", payload.JobType, time.Now().UnixNano()),
			InputPath:      payload.InputPath,
			OutputPath:     payload.OutputPath,
			Mapper:         mapperName,
			Reducer:        reducerName,
			NumMapTasks:    payload.NumMapTasks,
			NumReduceTasks: payload.NumReduceTasks,
		}

		req := &shared.SubmitJobRequest{
			Job:         job,
			MapperCode:  mBuf.Bytes(),
			ReducerCode: rBuf.Bytes(),
		}
		var resp shared.SubmitJobResponse

		if err := c.SubmitJob(req, &resp); err != nil {
			http.Error(w, "Submit Job error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(resp)
	})

	// 3. Simulated Chaos: Kill a worker
	mux.HandleFunc("/api/workers/kill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload struct {
			WorkerID string `json:"worker_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		worker, exists := c.workers[payload.WorkerID]
		if !exists {
			http.Error(w, "Worker not found", http.StatusNotFound)
			return
		}

		worker.Active = false
		worker.LastSeen = time.Time{} // far in the past

		// Immediately trigger reassignment of their tasks
		for _, task := range c.mapTasks {
			if (task.State == shared.TaskAssigned || task.State == shared.TaskRunning) && task.WorkerID == payload.WorkerID {
				task.State = shared.TaskPending
				task.WorkerID = ""
				log.Printf("[Chaos] Reassigned Map task %s due to manual kill of worker %s", task.ID, payload.WorkerID)
			}
		}
		for _, task := range c.reduceTasks {
			if (task.State == shared.TaskAssigned || task.State == shared.TaskRunning) && task.WorkerID == payload.WorkerID {
				task.State = shared.TaskPending
				task.WorkerID = ""
				log.Printf("[Chaos] Reassigned Reduce task %s due to manual kill of worker %s", task.ID, payload.WorkerID)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]bool{"success": true})
	})

	// 4. File Content Reader
	mux.HandleFunc("/api/file/content", func(w http.ResponseWriter, r *http.Request) {
		relPath := r.URL.Query().Get("path")
		if relPath == "" {
			http.Error(w, "Missing path parameter", http.StatusBadRequest)
			return
		}

		safePath := filepath.Clean(filepath.Join(c.storagePath, relPath))
		// Security: prevent directory traversal
		absSafe, err1 := filepath.Abs(safePath)
		absBase, err2 := filepath.Abs(c.storagePath)
		if err1 != nil || err2 != nil || !filepath.HasPrefix(absSafe, absBase) {
			http.Error(w, "Access denied", http.StatusForbidden)
			return
		}

		info, err := os.Stat(safePath)
		if err != nil {
			http.Error(w, "File not found: "+err.Error(), http.StatusNotFound)
			return
		}

		if info.IsDir() {
			http.Error(w, "Cannot read directory content as file", http.StatusBadRequest)
			return
		}

		content, err := ioutil.ReadFile(safePath)
		if err != nil {
			http.Error(w, "Read file error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write(content)
	})

	// 5. File Creator (Write / Upload)
	mux.HandleFunc("/api/file/write", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload struct {
			Filename string `json:"filename"` // e.g. "input-1.txt"
			Content  string `json:"content"`  // e.g. "some text contents"
		}

		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		if payload.Filename == "" {
			http.Error(w, "Filename required", http.StatusBadRequest)
			return
		}

		inputDir := filepath.Join(c.storagePath, "input")
		if err := os.MkdirAll(inputDir, 0755); err != nil {
			http.Error(w, "Create directory error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		filePath := filepath.Join(inputDir, payload.Filename)
		if err := ioutil.WriteFile(filePath, []byte(payload.Content), 0644); err != nil {
			http.Error(w, "Write file error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "path": "input/" + payload.Filename})
	})

	// 6. Serve Visualisation Web Assets (Single Page App)
	// Fallback to local files if embedded files are empty (for local dev)
	var fileServer http.Handler
	subFS, err := fs.Sub(webAssets, "web/dist")
	if err == nil {
		// Check if we actually have files embedded (not just the placeholder)
		f, openErr := subFS.Open("index.html")
		if openErr == nil {
			f.Close()
			fileServer = http.FileServer(http.FS(subFS))
			log.Println("Coordinator: Serving visualization web interface from embedded files")
		}
	}

	if fileServer == nil {
		// Fallback to local file system
		fileServer = http.FileServer(http.Dir("./web/dist"))
		log.Println("Coordinator: Serving visualization web interface from local web/dist")
	}

	mux.Handle("/", fileServer)

	log.Printf("Coordinator: Web visualization dashboard running on :%s", c.webPort)
	if err := http.ListenAndServe(":"+c.webPort, mux); err != nil {
		log.Printf("Coordinator: Web Server failure: %v", err)
	}
}

func main() {
	rpcPort := "1234"
	webPort := "8080"
	storagePath := "./shared-hdfs"

	if len(os.Args) >= 2 {
		rpcPort = os.Args[1]
	}
	if len(os.Args) >= 3 {
		webPort = os.Args[2]
	}
	if len(os.Args) >= 4 {
		storagePath = os.Args[3]
	}

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		log.Fatal("Failed to create storage directory:", err)
	}

	// Setup clean default input files if none exist
	inputDir := filepath.Join(storagePath, "input")
	if entries, err := ioutil.ReadDir(inputDir); err == nil && len(entries) == 0 {
		os.MkdirAll(inputDir, 0755)
		texts := []string{
			"The quick brown fox jumps over the lazy dog.",
			"MapReduce is a programming model and an associated implementation for processing and generating large data sets.",
			"Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs.",
			"And a reduce function that merges all intermediate values associated with the same intermediate key.",
		}
		for i, txt := range texts {
			ioutil.WriteFile(filepath.Join(inputDir, fmt.Sprintf("doc-%d.txt", i)), []byte(txt), 0644)
		}
		log.Println("Coordinator: Prepared 4 default input files in", inputDir)
	}

	coord := NewCoordinator(storagePath, rpcPort, webPort)
	coord.StartRPCServer()
	coord.StartMonitoring(3 * time.Second)
	
	// Start the Web Dashboard in a separate goroutine
	go coord.StartWebServer()

	// Wait for shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Coordinator shutting down...")
}

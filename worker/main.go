package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
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

func getMapperByName(name string) (shared.Mapper, error) {
	switch name {
	case "WordCountMapper":
		return &shared.WordCountMapper{}, nil
	case "InvertedIndexMapper":
		return &shared.InvertedIndexMapper{}, nil
	default:
		return nil, fmt.Errorf("unknown mapper: %s", name)
	}
}

func getReducerByName(name string) (shared.Reducer, error) {
	switch name {
	case "WordCountReducer":
		return &shared.WordCountReducer{}, nil
	case "InvertedIndexReducer":
		return &shared.InvertedIndexReducer{}, nil
	default:
		return nil, fmt.Errorf("unknown reducer: %s", name)
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
		Address:  w.id, // we represent the address as the unique worker ID
	}
	var resp shared.RegisterResponse
	if err := client.Call(shared.CoordinatorServiceName+".RegisterWorker", req, &resp); err != nil {
		return fmt.Errorf("RPC error: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Error)
	}

	log.Printf("Worker %s: Registered successfully with coordinator", w.id)
	return nil
}

func (w *Worker) SendHeartbeat() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if !w.active {
			return
		}

		client, err := rpc.Dial("tcp", w.coordinator)
		if err != nil {
			log.Printf("Worker %s: Heartbeat failed (cannot connect): %v", w.id, err)
			continue
		}

		req := &shared.HeartbeatRequest{
			WorkerID: w.id,
		}
		var resp shared.HeartbeatResponse
		err = client.Call(shared.CoordinatorServiceName+".Heartbeat", req, &resp)
		client.Close()

		if err != nil {
			log.Printf("Worker %s: Heartbeat RPC error: %v", w.id, err)
		} else if !resp.Success {
			log.Printf("Worker %s: Coordinator does not recognize heartbeat, re-registering...", w.id)
			w.RegisterWithCoordinator()
		}
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
	log.Printf("Worker %s: Starting MAP task %s for job %s", w.id, task.ID, job.ID)
	
	// Report task as running
	if err := w.ReportTask(task.ID, job.ID, shared.TaskRunning, ""); err != nil {
		return fmt.Errorf("failed to report task running: %v", err)
	}

	// Resolve mapper by name
	mapper, err := getMapperByName(job.Mapper)
	if err != nil {
		return fmt.Errorf("failed to resolve mapper: %v", err)
	}

	// Read input directory
	entries, err := ioutil.ReadDir(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input directory %s: %v", inputPath, err)
	}

	// Filter directories & hidden files
	var inputFiles []os.FileInfo
	for _, entry := range entries {
		if !entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			inputFiles = append(inputFiles, entry)
		}
	}

	if len(inputFiles) == 0 {
		return fmt.Errorf("no input files found in directory %s", inputPath)
	}

	// Select input file using modulo index for safety
	inputFile := inputFiles[task.TaskIndex%len(inputFiles)]
	inputFilePath := filepath.Join(inputPath, inputFile.Name())
	
	content, err := os.ReadFile(inputFilePath)
	if err != nil {
		return fmt.Errorf("failed to read input file %s: %v", inputFilePath, err)
	}

	// Create intermediate directory
	intermediateDir := filepath.Join(outputPath, "intermediate")
	if err := os.MkdirAll(intermediateDir, 0755); err != nil {
		return fmt.Errorf("failed to create intermediate directory %s: %v", intermediateDir, err)
	}

	// Open intermediate files for each reducer
	files := make([]*os.File, job.NumReduceTasks)
	for r := 0; r < job.NumReduceTasks; r++ {
		filename := filepath.Join(intermediateDir, fmt.Sprintf("mr-%d-%d", task.TaskIndex, r))
		f, err := os.Create(filename)
		if err != nil {
			// Close any already opened files
			for _, opened := range files {
				if opened != nil {
					opened.Close()
				}
			}
			return fmt.Errorf("failed to create intermediate file %s: %v", filename, err)
		}
		files[r] = f
	}
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()

	// Execute mapper
	outputChan := make(chan shared.KeyValue, 1000)
	var wg sync.WaitGroup

	// Start writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for kv := range outputChan {
			// Hash key to find reduce bucket
			r := shared.Ihash(kv.Key) % job.NumReduceTasks
			fmt.Fprintf(files[r], "%s\t%s\n", kv.Key, kv.Value)
		}
	}()

	// Map complete file content (key is filename, value is content)
	mapper.Map(inputFile.Name(), string(content), outputChan)

	close(outputChan)
	wg.Wait()

	log.Printf("Worker %s: Completed MAP task %s", w.id, task.ID)
	
	// Report task as completed
	if err := w.ReportTask(task.ID, job.ID, shared.TaskCompleted, ""); err != nil {
		return fmt.Errorf("failed to report task completed: %v", err)
	}

	return nil
}

func (w *Worker) ExecuteReduceTask(task *shared.Task, job *shared.Job, reducerCode []byte, inputPath, outputPath string) error {
	log.Printf("Worker %s: Starting REDUCE task %s for job %s", w.id, task.ID, job.ID)

	// Report task as running
	if err := w.ReportTask(task.ID, job.ID, shared.TaskRunning, ""); err != nil {
		return fmt.Errorf("failed to report task running: %v", err)
	}

	// Resolve reducer by name
	reducer, err := getReducerByName(job.Reducer)
	if err != nil {
		return fmt.Errorf("failed to resolve reducer: %v", err)
	}

	intermediateDir := filepath.Join(outputPath, "intermediate")
	files, err := ioutil.ReadDir(intermediateDir)
	if err != nil {
		return fmt.Errorf("failed to read intermediate directory: %v", err)
	}

	// Group intermediate data by key for this reducer index (task.TaskIndex)
	keyValues := make(map[string][]string)
	suffix := fmt.Sprintf("-%d", task.TaskIndex)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		// Read files matching "mr-*-task.TaskIndex"
		if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), suffix) {
			filePath := filepath.Join(intermediateDir, file.Name())
			content, err := ioutil.ReadFile(filePath)
			if err != nil {
				log.Printf("Worker %s: Failed to read intermediate file %s: %v", w.id, file.Name(), err)
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
	}

	// Create output directory
	outputDir := filepath.Join(outputPath, "final")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %v", outputDir, err)
	}

	// Create output file for this reducer
	outputFilePath := filepath.Join(outputDir, fmt.Sprintf("part-%d", task.TaskIndex))
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %v", outputFilePath, err)
	}
	defer outputFile.Close()

	// Execute reducer for each unique key
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

	log.Printf("Worker %s: Completed REDUCE task %s", w.id, task.ID)

	// Report task as completed
	if err := w.ReportTask(task.ID, job.ID, shared.TaskCompleted, ""); err != nil {
		return fmt.Errorf("failed to report task completed: %v", err)
	}

	return nil
}

func (w *Worker) Run() {
	// Register with coordinator
	for {
		err := w.RegisterWithCoordinator()
		if err == nil {
			break
		}
		log.Printf("Worker %s: Failed to register with coordinator, retrying in 2s... Error: %v", w.id, err)
		time.Sleep(2 * time.Second)
	}

	// Start heartbeat routine
	go w.SendHeartbeat()

	// Main execution loop
	for {
		w.mu.Lock()
		active := w.active
		w.mu.Unlock()
		if !active {
			break
		}

		// Request task
		taskResp, err := w.RequestTask()
		if err != nil {
			log.Printf("Worker %s: Failed to request task: %v", w.id, err)
			time.Sleep(2 * time.Second)
			continue
		}

		if taskResp.Task == nil {
			// No tasks available, sleep and retry
			time.Sleep(1 * time.Second)
			continue
		}

		task := taskResp.Task
		job := taskResp.Job

		log.Printf("Worker %s: Received %s task %s (index %d) for job %s",
			w.id, task.TaskType, task.ID, task.TaskIndex, job.ID)

		// Execute the task
		var taskErr error
		if task.TaskType == shared.TaskMap {
			taskErr = w.ExecuteMapTask(task, job, taskResp.Mapper, taskResp.InputPath, taskResp.OutputPath)
		} else if task.TaskType == shared.TaskReduce {
			taskErr = w.ExecuteReduceTask(task, job, taskResp.Reducer, taskResp.InputPath, taskResp.OutputPath)
		}

		if taskErr != nil {
			log.Printf("Worker %s: Task %s execution failed: %v", w.id, task.ID, taskErr)
			w.ReportTask(task.ID, job.ID, shared.TaskFailed, taskErr.Error())
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

	// Start worker in background
	log.Printf("Starting worker %s, coordinator at %s", workerID, coordinator)
	go worker.Run()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	
	log.Printf("Worker %s: Shutting down gracefully...", workerID)
	worker.mu.Lock()
	worker.active = false
	worker.mu.Unlock()
}

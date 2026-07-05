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
	"time"

	"mapreduce/shared"
)

func main() {
	coordinatorAddr := flag.String("coordinator", "localhost:1234", "Coordinator address")
	inputPath := flag.String("input", "./shared-hdfs/input", "Input directory")
	outputPath := flag.String("output", "./shared-hdfs/output", "Output directory")
	numMapTasks := flag.Int("map", 4, "Number of map tasks")
	numReduceTasks := flag.Int("reduce", 2, "Number of reduce tasks")
	flag.Parse()

	// Ensure directories exist
	if err := os.MkdirAll(*inputPath, 0755); err != nil {
		log.Fatal("Failed to create input directory:", err)
	}
	if err := os.MkdirAll(*outputPath, 0755); err != nil {
		log.Fatal("Failed to create output directory:", err)
	}

	// Create sample input files if empty
	entries, err := ioutil.ReadDir(*inputPath)
	if err == nil && len(entries) == 0 {
		sampleTexts := []string{
			"Hello world this is a test of MapReduce",
			"Hello again world from a distributed worker",
			"This is another test of the MapReduce system in action",
			"The world is a beautiful place for distributed systems",
		}
		for i, text := range sampleTexts {
			filename := filepath.Join(*inputPath, fmt.Sprintf("input-%d.txt", i))
			if err := ioutil.WriteFile(filename, []byte(text), 0644); err != nil {
				log.Fatalf("Failed to create input file %s: %v", filename, err)
			}
		}
		log.Println("Created default sample input files in", *inputPath)
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

	// Connect to coordinator RPC
	log.Printf("Connecting to coordinator at %s...", *coordinatorAddr)
	client, err := rpc.Dial("tcp", *coordinatorAddr)
	if err != nil {
		log.Fatal("Failed to connect to coordinator:", err)
	}
	defer client.Close()

	// Create job details
	job := shared.Job{
		ID:             fmt.Sprintf("wordcount-%d", time.Now().UnixNano()),
		InputPath:      *inputPath,
		OutputPath:     *outputPath,
		Mapper:         "WordCountMapper",
		Reducer:        "WordCountReducer",
		NumMapTasks:    *numMapTasks,
		NumReduceTasks: *numReduceTasks,
	}

	// Submit job
	req := &shared.SubmitJobRequest{
		Job:         job,
		MapperCode:  mapperBuf.Bytes(),
		ReducerCode: reducerBuf.Bytes(),
	}

	var resp shared.SubmitJobResponse
	log.Println("Submitting WordCount job to Coordinator...")
	if err := client.Call(shared.CoordinatorServiceName+".SubmitJob", req, &resp); err != nil {
		log.Fatal("Failed to submit job:", err)
	}

	if !resp.Success {
		log.Fatal("Job submission failed:", resp.Error)
	}

	log.Printf("Successfully submitted job %s! Monitoring status...", resp.JobID)

	// Monitor job status loop
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

		j := statusResp.Job
		log.Printf("Job %s state: %s", j.ID, j.State)

		if j.State == shared.JobCompleted {
			log.Printf("Job %s completed successfully!", j.ID)
			break
		} else if j.State == shared.JobFailed {
			log.Fatalf("Job %s FAILED", j.ID)
		}
	}

	// Print results from final output partition files
	log.Println("\n=== MAPREDUCE WORD COUNT OUTPUT RESULTS ===")
	finalDir := filepath.Join(*outputPath, "final")
	outputFiles, err := ioutil.ReadDir(finalDir)
	if err != nil {
		log.Fatalf("Failed to read output final directory %s: %v", finalDir, err)
	}

	for _, file := range outputFiles {
		if file.IsDir() {
			continue
		}
		filePath := filepath.Join(finalDir, file.Name())
		content, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Printf("Failed to read output file %s: %v", file.Name(), err)
			continue
		}
		fmt.Printf("--- Output Partition File: %s ---\n", file.Name())
		fmt.Println(string(content))
	}
}

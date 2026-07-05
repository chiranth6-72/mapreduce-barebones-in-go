package shared

import (
	"encoding/gob"
	"fmt"
	"strings"
	"time"
)

// Job represents a complete MapReduce job
type Job struct {
	ID             string    `json:"id"`
	InputPath      string    `json:"input_path"`
	OutputPath     string    `json:"output_path"`
	Mapper         string    `json:"mapper"` // Name of mapper implementation
	Reducer        string    `json:"reducer"` // Name of reducer implementation
	NumMapTasks    int       `json:"num_map_tasks"`
	NumReduceTasks int       `json:"num_reduce_tasks"`
	State          JobState  `json:"state"`
	CreatedAt      time.Time `json:"created_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
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

func (s JobState) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", s.String())), nil
}

// TaskType represents the type of task
type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
)

func (t TaskType) String() string {
	switch t {
	case TaskMap:
		return "MAP"
	case TaskReduce:
		return "REDUCE"
	default:
		return "UNKNOWN"
	}
}

func (t TaskType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", t.String())), nil
}

// Task represents a single unit of work
type Task struct {
	ID          string     `json:"id"`
	JobID       string     `json:"job_id"`
	TaskType    TaskType   `json:"task_type"`
	TaskIndex   int        `json:"task_index"` // 0-based index for this task type
	State       TaskState  `json:"state"`
	WorkerID    string     `json:"worker_id"`
	AssignedAt  time.Time  `json:"assigned_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Error       string     `json:"error,omitempty"`
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

func (s TaskState) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", s.String())), nil
}

// KeyValue represents an intermediate key-value pair
type KeyValue struct {
	Key   string
	Value string
}

// WorkerInfo represents a registered worker
type WorkerInfo struct {
	ID           string    `json:"id"`
	Address      string    `json:"address"`
	LastSeen     time.Time `json:"last_seen"`
	Active       bool      `json:"active"`
	TasksHandled int       `json:"tasks_handled"`
}

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
		cleanWord := strings.ToLower(strings.Trim(word, ".,!?;:\"'()[]{}&*@#$%/\\<>"))
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
		cleanWord := strings.ToLower(strings.Trim(word, ".,!?;:\"'()[]{}&*@#$%/\\<>"))
		if cleanWord != "" {
			output <- KeyValue{Key: cleanWord, Value: key}
		}
	}
}

// InvertedIndexReducer implements Reducer for inverted index
type InvertedIndexReducer struct{}

func (r *InvertedIndexReducer) Reduce(key string, values <-chan string, output chan<- string) {
	// Collect all document IDs for this word, deduplicate them
	docsMap := make(map[string]bool)
	for docID := range values {
		docsMap[docID] = true
	}
	
	var docs []string
	for docID := range docsMap {
		docs = append(docs, docID)
	}
	// Output as comma-separated list
	output <- strings.Join(docs, ",")
}

func init() {
	gob.Register(&WordCountMapper{})
	gob.Register(&WordCountReducer{})
	gob.Register(&InvertedIndexMapper{})
	gob.Register(&InvertedIndexReducer{})
}

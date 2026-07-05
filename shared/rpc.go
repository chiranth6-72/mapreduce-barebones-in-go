package shared

// TaskRequest is sent by workers to request a task
type TaskRequest struct {
	WorkerID string
}

// TaskResponse is sent by coordinator in response to TaskRequest
type TaskResponse struct {
	Task       *Task
	Job        *Job
	Mapper     []byte // Serialized mapper code
	Reducer    []byte // Serialized reducer code
	InputPath  string
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
	Job         Job
	MapperCode  []byte
	ReducerCode []byte
}

// SubmitJobResponse is the coordinator's acknowledgment
type SubmitJobResponse struct {
	Success bool   `json:"success"`
	JobID   string `json:"job_id"`
	Error   string `json:"error"`
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

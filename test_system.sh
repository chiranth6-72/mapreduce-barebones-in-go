#!/usr/bin/env bash

# Clean up previous runs
rm -rf ./shared-hdfs
mkdir -p ./shared-hdfs/input

# Kill any existing processes on port 1234 or 8080
echo "Cleaning up ports..."
taskkill //F //IM coordinator.exe //T 2>/dev/null || true
taskkill //F //IM worker.exe //T 2>/dev/null || true

# Start Coordinator
echo "Starting Coordinator..."
./bin/coordinator.exe 1234 8080 ./shared-hdfs &
COORD_PID=$!
sleep 2

# Start 2 Workers
echo "Starting Workers..."
./bin/worker.exe mr-worker-1 localhost:1234 ./shared-hdfs &
WORKER1_PID=$!
./bin/worker.exe mr-worker-2 localhost:1234 ./shared-hdfs &
WORKER2_PID=$!
sleep 2

# Submit and execute WordCount job
echo "Submitting WordCount Job..."
./bin/wordcount.exe -coordinator localhost:1234 -input ./shared-hdfs/input -output ./shared-hdfs/output -map 4 -reduce 2

# Print status of final directory
echo "Final results directory:"
ls -la ./shared-hdfs/output/final/

# Kill processes
echo "Tearing down cluster..."
taskkill //F //PID $COORD_PID 2>/dev/null || kill -9 $COORD_PID 2>/dev/null || true
taskkill //F //PID $WORKER1_PID 2>/dev/null || kill -9 $WORKER1_PID 2>/dev/null || true
taskkill //F //PID $WORKER2_PID 2>/dev/null || kill -9 $WORKER2_PID 2>/dev/null || true

echo "Done!"

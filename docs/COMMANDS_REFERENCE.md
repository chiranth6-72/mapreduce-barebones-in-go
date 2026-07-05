# The "Where Do I Type This?" Guide

Here’s a quick-reference for the most common commands you'll need. No fluff, just the good stuff.

---

## 🏗️ Building (The "Make it Work" Phase)

Before you run, you gotta build.

```bash
# Get the web dashboard ready (scrambled & shrunk)
npm run build

# Compile the Go binaries
go build -o bin/coordinator ./coordinator
go build -o bin/worker ./worker
go build -o bin/wordcount ./examples/wordcount
```

## 🚀 Running (The "Go Time" Phase)

### The Container Way (Recommended)
This starts 1 boss and 4 workers in the background.
```bash
podman compose up -d --build --scale mr-worker=4
```

### The Manual Way (For Debugging)
If you want to run it on your bare machine:
```bash
# Start the boss
./bin/coordinator 1234 8080 ./shared-hdfs

# Start a worker (in another terminal)
./bin/worker w-1 localhost:1234 ./shared-hdfs
```

## 📊 Monitoring (The "What's Happening?" Phase)

- **Web Dashboard:** Open `http://localhost:8080`
- **Container Logs:** `podman compose logs -f`
- **Check Processes:** `podman ps`

## 🧪 Testing (The "Did I Break It?" Phase)

We have a "magic" script that cleans up, builds everything, starts a cluster, runs a job, and shows you the results.
```bash
./test_system.sh
```

## 🧹 Cleaning Up (The "Leave No Trace" Phase)

Stop the containers and delete the temporary data.
```bash
podman compose down -v
rm -rf ./shared-hdfs
```

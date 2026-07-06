# 🌐 MapReduce in Action: A Distributed Engine with a Live Dashboard

*Built with 💻 by Chiranth and his humble copilot, Pi.*

Stop reading dry papers and start breaking things. This is a production-grade Distributed MapReduce framework built in Go, complete with a real-time, "glowing" visualization dashboard.

## 🚀 What is this?
It's a "Master-Worker" cluster that processes massive amounts of data in parallel. It handles the hard stuff—task scheduling, data shuffling, and worker crashes—so you don't have to.

### The "Cool Parts":
- **Live Topology Map:** See your cluster nodes radially positioned in real-time.
- **Chaos Mode:** Click a button to "kill" a worker and watch the cluster heal itself instantly.
- **Scrambled Frontend:** The dashboard code is minified and obfuscated (standard industry practice).
- **Simulated DFS:** A shared-storage layer that acts like a mini Google File System.

---

## 🛠️ Quick Start (The 30-Second Version)

1. **Build the dashboard:**
   ```bash
   npm install && npm run build
   ```
2. **Launch the cluster (requires Podman or Docker):**
   ```bash
   podman compose up -d --build --scale mr-worker=4
   ```
3. **See the magic:** Open `http://localhost:8080` in your browser.

---

## 📂 What's in the Box?

- **`coordinator/`**: The "Boss" node. Manages state and serves the web UI.
- **`worker/`**: The "Muscle." Statistically pulls tasks and crunches numbers.
- **`shared/`**: Common types and the hashing logic that keeps data organized.
- **`examples/`**: Ready-to-run jobs like **Word Count** and **Inverted Index**.

---

## 📖 The "Deep Dive" Docs

We wrote these like we're talking to a fellow dev. Check them out:

### 📘 Architectural Chapters
Deep-dive technical walkthroughs of each component, with algorithm analysis, state diagrams, and trade-off notes:
- 🧠 **[Chapter 1: Coordinator](./docs/chapters/ch1_coordinator.md)** — Job lifecycle, task scheduling, heartbeat monitoring, fault tolerance & RPC design.
- 🔧 **[Chapter 2: Worker](./docs/chapters/ch2_worker.md)** — Map task execution, FNV-1a hash partitioning, intermediate TSV chunking, polling loop.
- 🔀 **[Chapter 3: Shuffle & Reduce](./docs/chapters/ch3_shuffle_and_reduce.md)** — Implicit network partitioning, in-memory key grouping, reduce execution, output format.

### 📋 Reference Docs
- 🗺️ **[Architecture](./docs/ARCHITECTURE.md):** How the pieces fit together.
- 🛡️ **[Fault Tolerance](./docs/FAULT_TOLERANCE.md):** Why it doesn't crash when nodes die.
- 🐳 **[Containers](./docs/CONTAINER_GUIDE.md):** A beginner's guide to Podman/Docker.
- 🌋 **[Chaos Playbook](./docs/CHAOS_TESTING.md):** How to intentionally break the cluster.
- 📡 **[API Docs](./docs/API_DOCUMENTATION.md):** For when you want to script the boss.

---

## 🧪 Run the Full System Test
Want to see it work without clicking around? Run our automated integration script:
```bash
./test_system.sh
```
It builds everything, boots the cluster, runs a job, and shows you the final word counts in seconds.

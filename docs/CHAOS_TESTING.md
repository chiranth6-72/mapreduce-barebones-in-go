# The "Break Things" Playbook (Chaos Engineering)

Software usually works when everything is perfect. Distributed systems have to work when things are *falling apart*. This guide is about intentionally breaking the cluster to see how it heals.

---

## Scenario 1: "The Rage Quit" (Worker Crash)

**Goal:** See what happens when a worker disappears in the middle of a big job.

1. **Start a big job:** Go to the dashboard, set mappers to 10 and reducers to 4. Hit **Launch**.
2. **Commit the crime:** Head to the **Cluster Topology** tab. Wait until the progress hits about 30%, then find an active worker and smash the **Simulate Crash** button.
3. **The Reveal:** Watch the task grid. The tasks that were "Running" on that worker will instantly turn back to "Pending" (Gray). A second later, a different worker will "steal" that task and start it over.
4. **The Win:** The job still finishes. No data lost. No manual restart needed.

---

## Scenario 2: "The Ghost in the Machine" (Network Partition)

**Goal:** Simulate a worker that is still running but can't talk to the boss anymore (the "Zombie" state).

1. **Isolate a worker:** If you're using Podman/Docker, find a worker container and disconnect it from the network:
   `podman network disconnect mapreduce-net <worker_container_name>`
2. **Watch the Clock:** The worker is still "Active" in the Coordinator's mind... for now.
3. **The Timeout:** After 9 seconds, the Coordinator's watchdog will realize the worker hasn't "called home" (Heartbeat failure). It marks the worker as **Dead** and recycles its tasks.
4. **The Cleanup:** Even if that worker comes back later, the Coordinator will tell it to re-register because its previous session is expired.

---

## Scenario 3: "Data Integrity Check"

**Goal:** Prove that re-running a task doesn't result in "double data."

1. **Run a job** on a small text file.
2. **Kill the worker** specifically during the **Reduce** phase.
3. **The Recovery:** The new worker will restart that specific Reduce task.
4. **The Proof:** Because we use "Truncate-on-Create" for our output files, the partial data from the first (failed) attempt is wiped out, and the final `part-X` file remains perfectly accurate. No duplicate word counts!

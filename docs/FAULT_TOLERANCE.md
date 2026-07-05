# Resilience: Handling the "Oh Crap" Moments

In a distributed system, stuff breaks. Hard drives fail, RAM gets corrupted, and network cables get tripped over. Here is how we make sure your data doesn't disappear when the world is on fire.

---

## 1. The "I'm Still Alive" Loop (Heartbeats)
Every worker has a simple job: call the Coordinator every 1 second just to say "I'm still here!" 

If the Coordinator doesn't hear from a worker for **9 seconds**, it assumes the worst. It doesn't wait for a "Goodbye" message (because dead workers don't talk). It just assumes that node is gone and moves on.

## 2. Re-rolling the Dice (Task Reassignment)
When a worker dies, any task it was working on is now "orphaned."
- **Map Tasks:** We reset them to `PENDING`. Since the input data is on our shared storage, the next worker just picks up where the last one *started*.
- **Reduce Tasks:** Same deal. Since all the intermediate "Map" results were saved to the shared storage, a new worker can just re-read them and finish the job.

## 3. Idempotency (The "Do It Again" Rule)
The biggest fear in distributed systems is doing the same thing twice and getting different results (or double data). We solve this with **Idempotency**:
- **Deterministic Hashing:** We use FNV-1a hashing. A word like "Apple" will *always* go to Reducer #1. It doesn't matter which worker does the math.
- **Clean Slates:** Every time a task starts, it creates its output file from scratch (Truncate). If a task failed 90% of the way through, the retry just overwrites the garbage data with the correct stuff.

## 4. No Single Point of Failure?
*Self-correction:* Currently, the **Coordinator** is the single point of failure. If the boss dies, the cluster stops. In a massive production setup (like Google), you'd have a backup Coordinator waiting in the wings. For this project, we kept it simple: Keep the Coordinator lightweight so it almost never crashes!

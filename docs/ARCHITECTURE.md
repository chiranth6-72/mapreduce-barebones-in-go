# The 10,000 Foot View: How it Works

So, you want to know what's happening under the hood? This engine is built on the classic "Master-Worker" pattern. It's designed to be simple, pluggable, and resilient.

## The Big Picture

We've got one **Coordinator** (the boss) and as many **Workers** (the muscle) as you want to throw at it. They talk over Go's internal RPC system because it's faster and less "chatty" than standard REST for internal cluster talk.

```
          [ Browser Dashboard ] <--- (JSON API / WebSockets-ish)
                   |
            +------+-------+
            | Coordinator  | <--- (The Scheduler & Source of Truth)
            +--------------+
             /      |       \
      [Worker]   [Worker]   [Worker]  <--- (The Compute Muscle)
         \          |          /
          +---------+---------+
          |   Shared Storage  | <--- (Where the data lives)
          +-------------------+
```

## The "Brain" (Coordinator)
The Coordinator is basically a state machine. It doesn't do any heavy lifting (mapping or reducing). Its only jobs are:
- **Watching the Clock:** Tracking which workers are slacking off or dead.
- **Handing out Tickets:** Giving workers a `Task` when they ask for one.
- **Keeping Score:** Transitioning the job from `MAPPING` -> `SHUFFLING` -> `REDUCING`.

## The "Muscle" (Workers)
Workers are simple. They wake up, register with the boss, and then enter a loop:
1. "Hey Boss, got any work?"
2. If yes: "Got it, I'm on it." (Executes Map or Reduce).
3. "Hey Boss, I'm done with Task-X."
4. Repeat.

## Why Shared Storage?
In a real Google-scale setup, they use GFS. We simulate this with a shared folder (`shared-hdfs`). It's the "cheat code" that lets any worker pick up the pieces if another worker crashes. If a mapper dies halfway through, we don't care—the next worker just reads the same input file from the shared mount.

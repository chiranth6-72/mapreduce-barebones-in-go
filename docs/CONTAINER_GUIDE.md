# Containers 101: Docker & Podman for the Rest of Us

If you've never touched a container before, don't sweat it. Think of this as a "quick start" to get you up and running without needing a PhD in DevOps.

## 1. What's the deal with Containers?

Imagine you built a cool LEGO set at home. You want to show it to a friend, but if you carry it over, pieces might fall off, or your friend might not have the right baseplate.

A **container** is like putting that LEGO set in a specialized box with everything it needs (the baseplate, the instructions, and the bricks). You hand the box to your friend, and it looks *exactly* the same on their table as it did on yours.

In our MapReduce world:
- **Isolation:** Each worker thinks it's the only thing running. It has its own "name" (hostname) and its own space.
- **Networking:** Containers can talk to each other using names like `mr-coordinator` instead of messy IP addresses.
- **Shared Folders:** We use a "Volume" (a shared folder) so every container can see the same data files at once.

## 2. Podman vs. Docker (The "Rootless" Revolution)

You're probably using **Podman** because it's awesome and secure.
- **Docker** usually runs as a "boss" process (root) in the background.
- **Podman** is "rootless"—it runs just like any other app on your computer. This is safer, but it means sometimes you have to be careful with file permissions. 
- *Pro Tip:* That `:z` you see in our `docker-compose.yml` is the magic "permission fixer" for systems like Fedora or RHEL.

## 3. The "Recipe" (Dockerfiles)

We have two recipes:
1. **The Coordinator:** This one is a "multi-stage" build. It first uses Node.js to scramble and shrink the dashboard code, then switches to Go to build the brain, and finally puts it all in a tiny, lightweight box.
2. **The Worker:** A simpler recipe that just compiles the Go code and waits for orders.

## 4. The "Conductor" (Compose)

Running 5 containers manually is a pain. `docker-compose.yml` (or `podman-compose`) is our script that says: "Hey, start one Coordinator and four Workers, and make sure they can all see the `shared-hdfs` folder."

## 5. Cheat Sheet

| Action | The Command |
| :--- | :--- |
| **Start Everything** | `podman compose up -d --build --scale mr-worker=4` |
| **Check Status** | `podman ps` |
| **See Logs** | `podman compose logs -f` |
| **Kill Everything** | `podman compose down -v` |

**Gotcha:** If you get a "file not found" error when starting, make sure you've created the `shared-hdfs` folder on your computer first!

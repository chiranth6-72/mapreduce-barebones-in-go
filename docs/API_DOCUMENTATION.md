# The "Talking to the Boss" API

The Coordinator isn't just a brain; it's also a web server. If you want to build your own dashboard or script some automation, here is how you talk to it. It lives on port `8080`.

---

## 1. Get the Big Picture
**GET `/api/state`**

This is the "god view." It returns a massive JSON object with everything: who's online, what jobs are running, and what files are in the storage.

```bash
curl http://localhost:8080/api/state
```

## 2. Launch a Job
**POST `/api/submit`**

Tell the cluster to start crunching numbers.
```json
{
  "job_type": "wordcount",
  "num_map_tasks": 4,
  "num_reduce_tasks": 2
}
```
*Note:* You can also pick `invertedindex` if you're feeling fancy.

## 3. Be the Chaos Monkey
**POST `/api/workers/kill`**

Want to see the cluster sweat? Kill a worker manually.
```json
{
  "worker_id": "mr-worker-1"
}
```
The Coordinator will realize the worker is "gone" and immediately recycle its tasks to someone else.

## 4. Peek at Files
**GET `/api/file/content?path=input/doc-0.txt`**

Ever wonder what's actually inside those partition files? This endpoint lets you read any file in the `shared-hdfs` folder as raw text.

## 5. Upload Data
**POST `/api/file/write`**

Don't want to use our sample data? Upload your own text files to run jobs against.
```json
{
  "filename": "my-cool-story.txt",
  "content": "Once upon a time in a distributed cluster..."
}
```

---

**Pro Tip:** If you want the full technical details (types, formats, etc.), check out the [OpenAPI Spec](./openapi.yaml) and throw it into [Swagger Editor](https://editor.swagger.io/).

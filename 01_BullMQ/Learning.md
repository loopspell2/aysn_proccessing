# BullMQ Learning Notes (Quick Revision)

Folder: `Asyn_Proccessing/01_BullMQ` | Install packages at the **root** folder.

---

## 1. Why a queue?

Slow work (email, PDF) should not block the API. Add a job, respond fast, let a worker process it later. Jobs live in Redis, so they survive crashes.

```
API --add job--> Queue (Redis) --pick--> Worker
```

**Rule:** never do slow work inside the API request.

## 2. Setup (Docker + Redis)

`docker-compose.yml`:

```yaml
services:
  redis:
    image: redis:7-alpine
    container_name: bullmq-redis
    ports: ["6379:6379"]
    command: redis-server --appendonly yes --maxmemory-policy noeviction
    volumes: [redis-data:/data]
volumes:
  redis-data:
```

- `appendonly yes`: data survives restart.
- `noeviction`: Redis must never delete jobs when memory is full.
- Check: `docker exec -it bullmq-redis redis-cli ping` -> `PONG`

```powershell
npm install bullmq ioredis      # ioredis is NOT bundled, install it
```

## 3. Queue (Producer) | `producer.js`

```js
const { Queue } = require("bullmq");
const queue = new Queue("email-queue", { connection });
await queue.add("send-welcome-email", { to: "a@b.com" });
```

Job sits in Redis (`bull:email-queue:*`) until a worker takes it.

## 4. Worker | `worker.js`

```js
const { Worker } = require("bullmq");
const worker = new Worker(
  "email-queue",
  async (job) => {
    console.log(job.data);
    return { sent: true };
  },
  { connection },
);
```

- Queue name must match the producer's.
- Producer and worker are separate processes, linked only by Redis.
- Events: `worker.on("completed" | "failed" | "progress", ...)`

## 5. Retries and backoff

```js
await queue.add("job", data, {
  attempts: 3,
  backoff: { type: "exponential", delay: 1000 }, // 1s, 2s, 4s
});
```

Exponential backoff avoids hammering a service that is already down.

## 6. Permanently failed jobs

After all attempts, the job goes to the **failed** set and is not retried again.

| Option            | How                                                                      |
| ----------------- | ------------------------------------------------------------------------ |
| Dead letter queue | Copy to another queue inside the `failed` handler                        |
| Alert             | Slack/email in the final-failure check                                   |
| Manual retry      | `job.retry()` (gives only **one** more try, `attemptsMade` is not reset) |
| Auto-cleanup      | `removeOnFail: { age: 86400 }`                                           |

```js
worker.on("failed", async (job, err) => {
  if (job.attemptsMade >= job.opts.attempts) {
    await deadQueue.add(
      "dead",
      { originalData: job.data, reason: err.message },
      { jobId: `dead-${job.id}` },
    ); // jobId prevents duplicate DLQ entries
  }
});
```

Retry script: `const failed = await queue.getFailed(); for (const j of failed) await j.retry();`

## 7. Delayed jobs (run once, later) | `delayed-producer.js`

```js
await queue.add("reminder", data, { delay: 10000 }); // after 10s
```

## 8. Repeating jobs (cron) | `repeat-producer.js`

```js
await queue.upsertJobScheduler(
  "daily-report",
  { every: 5000 }, // or { pattern: "0 9 * * *" }
  { name: "send-report", data: {} },
);
```

Unique scheduler id = no duplicates. Schedule is stored in Redis. Stop it by flushing Redis or removing the scheduler.

## 9. Concurrency

How many jobs **one worker** runs at once (default 1). Goes in **Worker** options, not Queue.

```js
new Worker("email-queue", processor, { connection, concurrency: 5 });
```

Good for I/O jobs. Keep low for CPU-heavy jobs. Too high overloads DB/API.

## 10. Rate limiter

Caps how many jobs **start** per time window, shared across all workers.

```js
{ concurrency: 5, limiter: { max: 2, duration: 1000 } } // 2 jobs/sec
```

- Limiter never makes things faster, only slower (to respect API limits).
- 10 jobs x 2s, concurrency 5: ~4s. With limiter 2/sec: ~6s.
- `concurrency` = your server's capacity. `limiter` = the outside world's capacity.

## 11. Progress tracking

```js
await job.updateProgress(50); // number
await job.updateProgress({ step: 2, total: 3, message: "PDF" }); // object
await job.log("Step 1 done"); // history, shown in dashboard
worker.on("progress", (job, p) => console.log(p));
```

`progress` holds only the latest value. Use `job.log` for history.

## 12. Queue status | `queue-status.js`

```js
await queue.getJobCounts("waiting", "active", "completed", "failed", "delayed");
```

Growing `waiting` = workers cannot keep up.

## 13. Dashboard (Bull Board) | `dashboard.js`

```powershell
npm install express @bull-board/api @bull-board/express
```

```js
createBullBoard({ queues: [new BullMQAdapter(queue)], serverAdapter });
app.use("/admin/queues", serverAdapter.getRouter());
```

Open `http://localhost:3000/admin/queues`. Customize via `uiConfig`, `readOnlyMode`, `allowRetries`, `description`. **Protect it with auth in production.**

## 14. Job states

| State            | Meaning                                   |
| ---------------- | ----------------------------------------- |
| waiting          | Ready to be picked                        |
| active           | Being processed                           |
| completed        | Done                                      |
| failed           | Attempts exhausted                        |
| delayed          | Waiting for time (delay or retry backoff) |
| prioritized      | Waiting, ordered by priority              |
| waiting-children | Parent waiting for child jobs             |
| paused           | Queue paused                              |

`stalled` is an **event**, not a state. Check state: `await job.getState()`.
Retry flow: active -> delayed -> waiting -> active.

## 15. Memory cleanup

By default BullMQ keeps every completed/failed job forever.

```js
{ removeOnComplete: { age: 3600, count: 100 }, removeOnFail: { age: 86400 } }
// or removeOnComplete: true
```

Set once: `new Queue(name, { connection, defaultJobOptions: {...} })`.
Catch: once removed, the `jobId` is freed and can be added again.

## 16. Stalled jobs and locks

Worker sets a **lock** in Redis (30s) and renews it (~every 15s). If the worker dies, the lock expires, BullMQ marks the job stalled and moves it back to **waiting**.

```js
{ lockDuration: 30000, stalledInterval: 30000, maxStalledCount: 1 }
```

- Gives **at-least-once** processing: a job may run twice. Make jobs idempotent.
- A CPU-heavy job blocking the event loop cannot renew its lock, so it runs twice.

Raw Redis equivalent:

```
SET lock:job:1 token NX PX 30000          # acquire
EVAL "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('pexpire',KEYS[1],ARGV[2]) else return 0 end" 1 lock:job:1 token 30000   # renew
EVAL "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end" 1 lock:job:1 token                      # release
```

Token check stops a slow old worker from touching a new worker's lock.

## 17. Graceful shutdown

```js
async function shutdown(signal) {
  await worker.close(); // stop taking jobs, wait for active ones
  process.exit(0);
}
process.on("SIGINT", () => shutdown("SIGINT")); // Ctrl+C, PM2
process.on("SIGTERM", () => shutdown("SIGTERM")); // Docker, Kubernetes
```

**Rule:** platform timeout > longest job duration.

| Platform   | Fix                                                                     |
| ---------- | ----------------------------------------------------------------------- |
| Docker     | `CMD ["node", "worker.js"]` (not `npm start`); `stop_grace_period: 60s` |
| PM2        | `kill_timeout: 60000` (default is 1.6s)                                 |
| Kubernetes | `terminationGracePeriodSeconds: 60` (default 30s)                       |

If force-killed, stalled recovery still runs the job again.

## 18. Duplicate jobs

- **Queue level:** custom `jobId` (same id is ignored while the job exists).
  ```js
  await queue.add("welcome", data, { jobId: "welcome-arpit@example.com" });
  ```
  Examples: `welcome-<userId>` (once forever), `report-<userId>-<date>` (once per day).
- **Execution level (real safety net):** check a DB log or use an idempotency key before doing the work.
- Use both for money or user-facing messages.

## 19. Shared config | `config.js`

```js
const connection = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
};
const QUEUES = { EMAIL: "email-queue", DEAD: "email-dead-letter" };
module.exports = { connection, QUEUES };
```

Share the **plain config object**, not an ioredis instance. Shared queue names prevent typos.

## 20. Resumable step-based jobs

Save the step in job data and skip finished steps on restart.

```js
let step = job.data.step || 1;
if (step === 1) {
  /* work */ await job.updateData({ ...job.data, step: 2 });
  step = 2;
}
if (step === 2) {
  /* work */ await job.updateData({ ...job.data, step: 3 });
  step = 3;
}
```

Save the step **after** finishing it, so each step must be idempotent. Keep saved data small.

## 21. Flows (`waiting-children`) | `flow-producer.js`

Parent waits until all children finish. Nest to chain: deepest child runs first.

```js
const flow = new FlowProducer({ connection });
await flow.add({
  name: "send-email",
  queueName: "q",
  data: {},
  children: [
    {
      name: "generate-pdf",
      queueName: "q",
      data: {},
      children: [{ name: "validate", queueName: "q", data: {} }],
    },
  ],
});
// order: validate -> generate-pdf -> send-email
```

Pass data between steps: `Object.values(await job.getChildrenValues())[0]`

Child failure options (on the **child** `opts`):

| Option                            | Result                             |
| --------------------------------- | ---------------------------------- |
| none                              | Parent stuck in `waiting-children` |
| `failParentOnFailure: true`       | Parent fails too                   |
| `ignoreDependencyOnFailure: true` | Parent runs anyway                 |

A failed step retries alone, finished steps do not re-run. Steps can use different queues and workers.

## 22. Pause / resume

```js
await queue.pause();
await queue.isPaused();
await queue.resume(); // whole queue, all workers
await worker.pause();
await worker.resume(); // one worker only
```

Active jobs finish normally. Use for maintenance, downstream outages, floods of bad jobs.

## 23. Mini project | `project/`

Express API (`POST /signup`) adds a flow `validate -> generate-pdf -> send-email`, worker processes it, Bull Board shows it. Uses: shared config, flows, retries, `jobId` dedupe, cleanup options, graceful shutdown.

---

## Gotchas I hit

- `ioredis` must be installed separately.
- `concurrency` belongs in **Worker** options, not Queue options.
- `job.retry()` does not reset `attemptsMade`.
- PowerShell breaks curl JSON quotes. Use `Invoke-RestMethod` with `-Body '{"a":1}'`.
- Old dashboard and new API both use port 3000, so stop one.
- `flushall` clears everything in Redis. Use it only for testing.

## 5 golden rules

1. Never do slow work inside the API request. Queue it.
2. Jobs can run twice (stalls, retries), so make them idempotent.
3. Always set `removeOnComplete` and `removeOnFail`.
4. Handle `SIGTERM` and `SIGINT`; platform timeout > longest job.
5. Watch `waiting` counts. If they keep growing, add workers or concurrency.

## File reference

| File                                                   | Topic                                            |
| ------------------------------------------------------ | ------------------------------------------------ |
| `docker-compose.yml`                                   | Redis setup                                      |
| `producer.js`                                          | Queue, add job, retry options                    |
| `worker.js`                                            | Worker, concurrency, limiter, progress, shutdown |
| `retry-failed.js`                                      | Manual retry of failed jobs                      |
| `delayed-producer.js`                                  | Delayed job                                      |
| `repeat-producer.js`                                   | Repeating/cron job                               |
| `bulk-producer.js`                                     | `addBulk` for concurrency tests                  |
| `queue-status.js`                                      | Job counts                                       |
| `dashboard.js`                                         | Bull Board                                       |
| `duplicate-test.js`                                    | `jobId` dedupe                                   |
| `config.js`                                            | Shared connection and queue names                |
| `flow-producer.js`, `flow-worker.js`, `flow-status.js` | Chained flow                                     |
| `project/`                                             | Signup mini project                              |

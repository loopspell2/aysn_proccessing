# RabbitMQ Learning Notes (amqplib, Node.js)

Setup: Docker `rabbitmq:3-management`, AMQP on `5672`, UI on `15672`, shared `config.js` for URL, queue and exchange names.

Core flow: `Producer -> Exchange -> (Binding) -> Queue -> Consumer`

---

## 1. Why RabbitMQ, core concepts

RabbitMQ is a language-independent message broker (AMQP) with smart routing. BullMQ is a Node.js job library on Redis, with retries, delays and flows built in.

| Concept  | Meaning                        | BullMQ equivalent |
| -------- | ------------------------------ | ----------------- |
| Producer | Sends messages                 | `queue.add()`     |
| Exchange | Routes messages to queues      | none              |
| Binding  | Rule linking exchange to queue | none              |
| Queue    | Stores messages                | `Queue`           |
| Consumer | Processes messages             | `Worker`          |
| Ack      | "Done, delete it"              | job completes     |

Key points: you must ack or nack yourself. Retries, delay and failed set are built by you (TTL + DLX).

## 2. Docker setup + management UI

```yaml
services:
  rabbitmq:
    image: rabbitmq:3-management
    container_name: rabbitmq
    hostname: rabbitmq # fixed hostname keeps data across recreates
    ports: ["5672:5672", "15672:15672"]
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin123
    volumes: [rabbitmq_data:/var/lib/rabbitmq]
volumes:
  rabbitmq_data:
```

```powershell
docker compose up -d
docker logs rabbitmq --tail 20
```

Never run `docker compose down -v` unless you want to delete data.

## 3. First producer and consumer

Default exchange: `sendToQueue('name')` routes to the queue with that name.

```js
const conn = await amqp.connect(RABBIT_URL);
const ch = await conn.createChannel();
await ch.assertQueue("hello_queue");
ch.sendToQueue("hello_queue", Buffer.from(JSON.stringify({ text: "hi" })));

ch.consume("hello_queue", (m) => {
  if (!m) return; // consumer cancelled by broker
  console.log(JSON.parse(m.content.toString()));
  ch.ack(m);
});
```

Queues buffer messages while no consumer is running. `assertQueue` options must match on every call (else `PRECONDITION_FAILED`).

## 4. Acknowledgements

| Call                       | Meaning                                             |
| -------------------------- | --------------------------------------------------- |
| `ch.ack(m)`                | Done, delete                                        |
| `ch.nack(m, false, true)`  | Failed, requeue                                     |
| `ch.nack(m, false, false)` | Failed, drop or dead-letter                         |
| Consumer dies              | Broker redelivers (`m.fields.redelivered === true`) |

```js
try {
  await work();
  ch.ack(m);
} catch {
  ch.nack(m, false, !m.fields.redelivered);
} // retry once, then drop
```

`{ noAck: true }` auto-acks on delivery, so a crash loses the message. Use it only for data you can lose (metrics, pings). Never requeue a poison message blindly (infinite loop).

## 5. Durable queues and persistent messages

Both are needed to survive a broker restart.

```js
await ch.assertQueue("q", { durable: true });
ch.sendToQueue("q", body, { persistent: true });
```

| Queue       | Message    | After restart |
| ----------- | ---------- | ------------- |
| durable     | persistent | both survive  |
| durable     | transient  | message lost  |
| non-durable | any        | queue gone    |

amqplib queues are durable by default, but messages are not persistent by default. Use publisher confirms (Step 14) to know it reached disk.

## 6. Prefetch and work distribution

Without prefetch, RabbitMQ pushes messages round-robin regardless of speed. `prefetch(n)` caps unacked messages per consumer.

```js
await ch.prefetch(1); // BEFORE ch.consume
```

- `prefetch(1)`: fair for slow or heavy jobs.
- `10-50`: better throughput for tiny jobs.
- `0` or unset: unlimited, so every ready message is pushed into your process memory.
- Prefetch N means N messages processed in parallel (like BullMQ `concurrency`).

## 7. Exchanges

| Type    | Routes by                                   | Example      |
| ------- | ------------------------------------------- | ------------ |
| direct  | exact key match                             | `error`      |
| fanout  | ignores key, copies to all bound queues     | broadcast    |
| topic   | pattern, `*` = one word, `#` = zero or more | `orders.*`   |
| headers | message headers, `x-match` all or any       | `format=pdf` |

```js
await ch.assertExchange("events_topic", "topic", { durable: true });
const { queue } = await ch.assertQueue("", { exclusive: true }); // temp queue
await ch.bindQueue(queue, "events_topic", "orders.*");
ch.publish("events_topic", "orders.created", Buffer.from("x"));
```

No matching binding means the message is silently dropped (use `mandatory`, Step 14). Several consumers on one queue compete; for each to get a copy, give each its own queue. In PowerShell quote `#` and `*`.

## 8. Retries, delayed messages, TTL

No built-in retry or delay. Pattern: failed message goes to a **wait queue** (TTL, no consumer), then is dead-lettered back to the main queue.

```js
await ch.assertQueue("wait_q", {
  durable: true,
  arguments: {
    "x-message-ttl": 3000,
    "x-dead-letter-exchange": "", // default exchange
    "x-dead-letter-routing-key": "main_q", // back to the main queue
  },
});
// on failure:
ch.sendToQueue("wait_q", m.content, {
  persistent: true,
  headers: { attempts: n },
});
ch.ack(m);
```

Per-message TTL: `{ expiration: '5000' }`. Delayed message: publish straight to the wait queue.
Problem: expiry happens only at the queue head, so use one wait queue per delay tier. Queue args cannot change after creation.

## 9. Dead letter exchanges

Messages are dead-lettered when: nack/reject with `requeue=false`, TTL expires, or the queue is full (`x-max-length`).

```js
await ch.assertQueue("orders_q", {
  durable: true,
  arguments: {
    "x-dead-letter-exchange": "dlx_ex",
    "x-dead-letter-routing-key": "dead",
  },
});
ch.nack(m, false, false); // goes to the DLQ
```

The DLQ message carries an `x-death` header (reason, original queue, count). Without a DLX and a binding, dead messages are dropped. Alert when DLQ Ready > 0. Fix the bug before replaying.

## 10. Priority queues

```js
await ch.assertQueue("priority_q", {
  durable: true,
  arguments: { "x-max-priority": 10 },
});
ch.sendToQueue("priority_q", body, { priority: 9 });
```

Higher number means higher priority (opposite of BullMQ). FIFO within the same priority. Only reorders messages waiting in the queue, so use a small prefetch. Keep max priority at 5-10. Beware starvation of low priority.

## 11. Reconnection and connection/channel handling

amqplib does **not** reconnect automatically.

- One connection per process, reuse channels, never one channel per message.
- Separate channels (ideally connections) for publish and consume.
- Always attach `on('error')` and `on('close')`; an unhandled `error` crashes Node.

```js
async function start() {
  const conn = await connectWithRetry(); // exponential backoff
  conn.on("error", (e) => console.log(e.message));
  conn.on("close", () => setTimeout(start, 1000));
  const ch = await conn.createChannel();
  ch.on("close", () => conn.close().catch(() => {})); // channel died: force restart
}
```

Buffer messages while disconnected (outbox for critical data). Use heartbeats (`?heartbeat=30`) to detect dead connections. Add jitter to backoff.

## 12. Duplicate messages and idempotency

Delivery is at-least-once, so duplicates happen (crash before ack, retries, reconnects). Make processing idempotent.

```js
ch.sendToQueue("q", body, { persistent: true, messageId: "pay-1001" }); // stable ID
// consumer
if (processed.has(m.properties.messageId)) return ch.ack(m);
doWork();
processed.add(id);
ch.ack(m);
```

Production: record the ID in the same DB transaction as the work (unique key), or Redis `SET id 1 NX EX 86400`. Prefer naturally idempotent operations (`SET status='paid'`). Pass the ID as an idempotency key to external APIs.

## 13. Graceful shutdown

Stop taking new messages, finish in-flight ones, ack, then close.

```js
const { consumerTag } = await ch.consume(q, handler);
async function shutdown() {
  await ch.cancel(consumerTag); // no new messages
  while (inFlight > 0) await sleep(100); // finish current work
  await ch.close();
  await conn.close();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
```

Use a force-exit timer shorter than the platform kill timeout (Docker 10s, Kubernetes 30s). Ack before closing the channel. Skip reconnect logic while shutting down.

## 14. Publisher confirms

`publish` returning does not mean the broker has the message. A confirm channel makes the broker ack or nack.

```js
const ch = await conn.createConfirmChannel();
ch.on("return", (m) => console.log("unroutable", m.fields.routingKey));
ch.publish(ex, key, body, { persistent: true, mandatory: true });
await ch.waitForConfirms();
```

A confirm does not mean routed, so add `mandatory: true` plus the `return` handler. Batch then wait once for speed. On error or nack, re-publish with the same `messageId`. If `publish` returns `false`, wait for `drain`. For DB plus event consistency use the outbox pattern.

## 15. RPC pattern

Request/reply using `replyTo` (private reply queue) and `correlationId` (match reply to request).

```js
// client
const { queue: replyQ } = await ch.assertQueue("", { exclusive: true });
ch.sendToQueue("rpc_queue", Buffer.from("10"), {
  correlationId: id,
  replyTo: replyQ,
  expiration: "5000",
});
// server
ch.sendToQueue(m.properties.replyTo, Buffer.from(result), {
  correlationId: m.properties.correlationId,
});
ch.ack(m);
```

Always use a client timeout and request `expiration`. One reply queue per client. Return `{ error }` instead of silence. The server may run a request twice after a crash, so keep it idempotent. Prefer events over RPC when you do not need the answer now.

## 16. Monitoring and common problems

Watch: **Ready** (backlog), **Unacked** (stuck handlers), **Consumers** (0 with Ready > 0), **DLQ Ready** (> 0), **alarms** (memory or disk).

```powershell
docker exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
$b64 = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:admin123"))
Invoke-RestMethod http://localhost:15672/api/queues -Headers @{Authorization="Basic $b64"}
```

Prometheus metrics are on port `15692`.

| Problem                  | Fix                                                 |
| ------------------------ | --------------------------------------------------- |
| Ready keeps growing      | Add consumers, tune prefetch                        |
| Unacked stuck            | Timeouts, ack in every code path                    |
| Consumers = 0, app alive | Channel `close` handler + reconnect                 |
| Messages vanish          | `mandatory` + `return` handler                      |
| Publishers blocked       | Free disk or memory, handle `blocked` / `unblocked` |
| Connection count grows   | One connection per process                          |
| DLQ grows silently       | Alert on DLQ Ready                                  |

## 17. Mini project: order processing

One `order.*` event published to a topic exchange; inventory and email services each have their own queue.

- Topic exchange `shop_topic`, queues bound with `order.created` and `order.*`.
- Producer uses a confirm channel, `mandatory`, and a stable `messageId`.
- Workers: `prefetch(1)`, idempotency check, retry through a `.wait` queue (3s TTL), max 3 attempts, then `nack(false,false)` to the DLQ.
- Graceful shutdown with in-flight tracking; exit on connection loss and let PM2/Docker restart.

```powershell
node .\02_RabbitMQ\shop_worker.js inventory
node .\02_RabbitMQ\shop_worker.js email
node .\02_RabbitMQ\shop_producer.js
node .\02_RabbitMQ\shop_dlq_reader.js
```

Production gaps: Redis or DB for dedup, confirm channel for retry publishes, per-service DLQ keys, outbox pattern.

---

## Quick comparison: BullMQ vs RabbitMQ

| Need                | BullMQ                     | RabbitMQ                           |
| ------------------- | -------------------------- | ---------------------------------- |
| Retries and backoff | `attempts`, `backoff`      | TTL wait queue + DLX               |
| Delayed jobs        | `delay`                    | TTL wait queue (or plugin)         |
| Failed jobs         | failed set                 | dead letter queue                  |
| Priority            | `priority` (lower = first) | `x-max-priority` (higher = first)  |
| Concurrency         | `concurrency`              | `prefetch`                         |
| Dedup               | custom `jobId`             | `messageId` + your own check       |
| Graceful stop       | `worker.close()`           | `ch.cancel()` + wait for in-flight |
| Reconnect           | automatic (ioredis)        | you write it                       |
| Routing             | one queue per `add()`      | exchanges and bindings             |
| Return value        | `waitUntilFinished`        | RPC with `replyTo`                 |

## Golden rules

1. Manual ack in every code path; never `noAck` for important work.
2. Durable queue + persistent message + publisher confirms.
3. Always set a finite prefetch.
4. Make consumers idempotent (`messageId`).
5. Every queue that can fail needs a DLX, and the DLQ needs an alert.
6. One connection per process, handle `error` and `close`, reconnect with backoff.
7. Shut down gracefully: cancel, drain, ack, close.
s
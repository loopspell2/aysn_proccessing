const amqp = require('amqplib');
const crypto = require('crypto');
const { RABBIT_URL, QUEUES } = require('./config');

const TIMEOUT_MS = 5000;

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();

    // private reply queue, deleted when we disconnect
    const { queue: replyQueue } = await ch.assertQueue('', { exclusive: true });

    const waiting = new Map(); // correlationId -> { resolve, reject, timer }

    ch.consume(replyQueue, (m) => {
        if (!m) return;
        const w = waiting.get(m.properties.correlationId);
        if (!w) return; // late or unknown reply, ignore
        clearTimeout(w.timer);
        waiting.delete(m.properties.correlationId);
        w.resolve(JSON.parse(m.content.toString()));
    }, { noAck: true });

    function call(n) {
        return new Promise((resolve, reject) => {
            const correlationId = crypto.randomUUID();
            const timer = setTimeout(() => {
                waiting.delete(correlationId);
                reject(new Error(`RPC timeout for fib(${n})`));
            }, TIMEOUT_MS);
            waiting.set(correlationId, { resolve, reject, timer });

            ch.sendToQueue(QUEUES.RPC, Buffer.from(String(n)), {
                correlationId,
                replyTo: replyQueue,
                expiration: String(TIMEOUT_MS), // server skips requests nobody waits for
            });
        });
    }

    // several requests at the same time, matched by correlationId
    const results = await Promise.all([call(10), call(20), call(30)]);
    console.log('Results:', results);

    await ch.close();
    await conn.close();
}

main().catch((e) => { console.error('FAILED:', e.message); process.exit(1); });
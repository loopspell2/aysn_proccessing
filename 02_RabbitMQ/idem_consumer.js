const amqp = require('amqplib');
const fs = require('fs');
const path = require('path');
const { RABBIT_URL, QUEUES } = require('./config');

// Simple file-based store so it survives restarts. Use Redis/DB in production.
const STORE = path.join(__dirname, 'processed_ids.json');
const load = () => (fs.existsSync(STORE) ? new Set(JSON.parse(fs.readFileSync(STORE))) : new Set());
const processed = load();
const save = () => fs.writeFileSync(STORE, JSON.stringify([...processed]));

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.IDEMPOTENT, { durable: true });
    await ch.prefetch(1);
    console.log(`Waiting... already processed: ${processed.size}`);

    ch.consume(QUEUES.IDEMPOTENT, (m) => {
        if (!m) return;
        const id = m.properties.messageId;
        const order = JSON.parse(m.content.toString());

        if (processed.has(id)) {
            console.log(`SKIP duplicate ${id}`);
            return ch.ack(m); // ack so it leaves the queue
        }

        console.log(`CHARGE order ${order.orderId}: ${order.amount}`); // the real work
        processed.add(id);
        save();
        ch.ack(m);
    });
}

main().catch(console.error);
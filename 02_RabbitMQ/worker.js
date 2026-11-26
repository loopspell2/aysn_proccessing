const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

// usage: node worker.js <name> <delayMs> <prefetch>
const [name, delay, prefetch] = [process.argv[2], Number(process.argv[3]), Number(process.argv[4])];

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.WORK, { durable: true });

    if (prefetch > 0) await ch.prefetch(prefetch); // must be BEFORE consume
    console.log(`[${name}] delay=${delay}ms prefetch=${prefetch || 'unlimited'}`);

    let total = 0;
    ch.consume(QUEUES.WORK, async (m) => {
        if (!m) return;
        const { id } = JSON.parse(m.content.toString());
        await new Promise((r) => setTimeout(r, delay)); // simulate work
        ch.ack(m);
        total++;
        console.log(`[${name}] done #${id} (total ${total})`);
    });
}

main().catch(console.error);
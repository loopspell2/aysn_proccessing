const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.PRIORITY, {
        durable: true,
        arguments: { 'x-max-priority': 10 },
    });
    await ch.prefetch(1); // important: see problems below
    console.log('Waiting...');

    ch.consume(QUEUES.PRIORITY, (m) => {
        if (!m) return;
        console.log(`Got ${m.content.toString()} (priority ${m.properties.priority})`);
        ch.ack(m);
    });
}

main().catch(console.error);
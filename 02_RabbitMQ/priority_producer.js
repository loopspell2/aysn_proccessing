const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.PRIORITY, {
        durable: true,
        arguments: { 'x-max-priority': 10 },
    });

    const msgs = [
        { name: 'newsletter-1', priority: 1 },
        { name: 'newsletter-2', priority: 1 },
        { name: 'report', priority: 3 },
        { name: 'PAYMENT', priority: 9 },
        { name: 'newsletter-3', priority: 1 },
        { name: 'PASSWORD-RESET', priority: 10 },
    ];
    for (const m of msgs) {
        ch.sendToQueue(QUEUES.PRIORITY, Buffer.from(m.name), { persistent: true, priority: m.priority });
        console.log(`Sent ${m.name} (priority ${m.priority})`);
    }

    await ch.close();
    await conn.close();
}

main().catch(console.error);
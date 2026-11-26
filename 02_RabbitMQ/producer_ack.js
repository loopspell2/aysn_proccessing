const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.TASKS);

    const mode = process.argv[2];
    const tasks = mode === 'crash'
        ? [{ id: 5, type: 'crash' }]
        : [
            { id: 1, type: 'ok' },
            { id: 2, type: 'fail_once' },
            { id: 3, type: 'poison' },
            { id: 4, type: 'ok' },
        ];

    for (const t of tasks) {
        ch.sendToQueue(QUEUES.TASKS, Buffer.from(JSON.stringify(t)));
        console.log('Sent:', t);
    }
    await ch.close();
    await conn.close();
}

main().catch(console.error);
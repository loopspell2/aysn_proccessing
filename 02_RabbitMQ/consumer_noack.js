const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const mode = process.argv[2] === 'noack'; // true = auto-ack
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.TASKS);
    console.log(`Waiting... noAck=${mode}`);

    ch.consume(QUEUES.TASKS, (m) => {
        if (!m) return;
        const task = JSON.parse(m.content.toString());
        console.log(`Got #${task.id}, working... (crashing in 1s)`);

        setTimeout(() => {
            process.exit(1); // crash BEFORE finishing the work
        }, 1000);

        // note: no ch.ack() here, so we can see what the broker does
    }, { noAck: mode });
}

main().catch(console.error);
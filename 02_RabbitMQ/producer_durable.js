const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();

    await ch.assertQueue(QUEUES.DUR_PERSISTENT, { durable: true });
    await ch.assertQueue(QUEUES.DUR_TRANSIENT_MSG, { durable: true });
    await ch.assertQueue(QUEUES.TRANSIENT, { durable: false });

    for (let i = 1; i <= 2; i++) {
        const body = Buffer.from(JSON.stringify({ n: i }));
        ch.sendToQueue(QUEUES.DUR_PERSISTENT, body, { persistent: true });
        ch.sendToQueue(QUEUES.DUR_TRANSIENT_MSG, body, { persistent: false });
        ch.sendToQueue(QUEUES.TRANSIENT, body, { persistent: true });
    }
    console.log('Sent 2 messages to each of the 3 queues');

    await ch.close();
    await conn.close();
}

main().catch(console.error);
const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.WORK, { durable: true });

    for (let i = 1; i <= 20; i++) {
        ch.sendToQueue(QUEUES.WORK, Buffer.from(JSON.stringify({ id: i })), { persistent: true });
    }
    console.log('Sent 20 messages');

    await ch.close();
    await conn.close();
}

main().catch(console.error);
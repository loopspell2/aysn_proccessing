const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.SHUTDOWN, { durable: true });

    for (let i = 1; i <= 10; i++) {
        ch.sendToQueue(QUEUES.SHUTDOWN, Buffer.from(JSON.stringify({ id: i })), { persistent: true });
    }
    console.log('Sent 10 messages');

    await ch.close();
    await conn.close();
}

main().catch(console.error);
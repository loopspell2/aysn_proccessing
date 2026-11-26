const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();

    await ch.assertQueue(QUEUES.HELLO);
    console.log('Waiting for messages...');

    ch.consume(QUEUES.HELLO, (m) => {
        if (!m) return; // consumer was cancelled by the broker
        const data = JSON.parse(m.content.toString());
        console.log('Received:', data);
        ch.ack(m); // tell the broker "done", it deletes the message
    });
}

main().catch(console.error);
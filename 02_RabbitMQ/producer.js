const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();

    await ch.assertQueue(QUEUES.HELLO); // create the queue if missing

    const msg = { id: Date.now(), text: 'Hello RabbitMQ' };
    ch.sendToQueue(QUEUES.HELLO, Buffer.from(JSON.stringify(msg)));
    console.log('Sent:', msg);

    await ch.close();
    await conn.close();
}

main().catch(console.error);
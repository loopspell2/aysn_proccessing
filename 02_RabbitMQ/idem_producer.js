const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.IDEMPOTENT, { durable: true });

    const send = (id, body) =>
        ch.sendToQueue(QUEUES.IDEMPOTENT, Buffer.from(JSON.stringify(body)), {
            persistent: true,
            messageId: id,
        });

    send('pay-1001', { orderId: 1001, amount: 500 });
    send('pay-1002', { orderId: 1002, amount: 250 });
    send('pay-1001', { orderId: 1001, amount: 500 }); // duplicate!
    send('pay-1001', { orderId: 1001, amount: 500 }); // duplicate!
    console.log('Sent 4 messages (2 unique, 2 duplicates)');

    await ch.close();
    await conn.close();
}

main().catch(console.error);
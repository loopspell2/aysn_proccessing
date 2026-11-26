const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');
const setup = require('./dlx_setup');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await setup(ch);

    if (process.argv[2] === 'limit') {
        for (let i = 1; i <= 4; i++) {
            ch.sendToQueue(QUEUES.LIMITED, Buffer.from(JSON.stringify({ id: i })), { persistent: true });
        }
        console.log('Sent 4 messages to limited_q (max 2)');
    } else {
        for (const t of [{ id: 1, type: 'ok' }, { id: 2, type: 'bad' }, { id: 3, type: 'ok' }]) {
            ch.sendToQueue(QUEUES.ORDERS, Buffer.from(JSON.stringify(t)), { persistent: true });
        }
        console.log('Sent 3 orders');
    }
    await ch.close();
    await conn.close();
}

main().catch(console.error);
const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

const fib = (n) => (n < 2 ? n : fib(n - 1) + fib(n - 2));

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.RPC, { durable: false });
    await ch.prefetch(1);
    console.log('RPC server waiting...');

    ch.consume(QUEUES.RPC, (m) => {
        if (!m) return;
        const n = parseInt(m.content.toString(), 10);
        console.log(`request fib(${n}) correlationId=${m.properties.correlationId}`);

        let reply;
        try {
            reply = JSON.stringify({ result: fib(n) });
        } catch (e) {
            reply = JSON.stringify({ error: e.message });
        }

        ch.sendToQueue(m.properties.replyTo, Buffer.from(reply), {
            correlationId: m.properties.correlationId,
        });
        ch.ack(m);
    });
}

main().catch(console.error);
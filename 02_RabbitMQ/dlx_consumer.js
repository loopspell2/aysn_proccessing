const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');
const setup = require('./dlx_setup');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await setup(ch);
    await ch.prefetch(5);
    console.log('Waiting for orders...');

    ch.consume(QUEUES.ORDERS, (m) => {
        if (!m) return;
        const o = JSON.parse(m.content.toString());
        if (o.type === 'bad') {
            console.log(`#${o.id} REJECTED -> dead letter`);
            ch.nack(m, false, false); // requeue=false triggers the DLX
        } else {
            console.log(`#${o.id} OK`);
            ch.ack(m);
        }
    });
}

main().catch(console.error);
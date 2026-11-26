const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    let m;
    while ((m = await ch.get(QUEUES.DLQ, { noAck: false }))) {
        const d = m.properties.headers['x-death'][0];
        console.log(`DLQ: ${m.content.toString()} | reason=${d.reason} | from=${d.queue} | count=${d.count}`);
        ch.ack(m);
    }
    console.log('DLQ is empty');
    await ch.close();
    await conn.close();
}

main().catch(console.error);
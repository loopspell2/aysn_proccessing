const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    let m;
    while ((m = await ch.get(QUEUES.SHOP_DLQ, { noAck: false }))) {
        const deaths = (m.properties.headers['x-death'] || []).map((d) => `${d.queue}:${d.reason} x${d.count}`);
        console.log(`DLQ: ${m.properties.messageId} ${m.content.toString()} | ${deaths.join(', ')}`);
        ch.ack(m);
    }
    console.log('DLQ is empty');
    await ch.close();
    await conn.close();
}

main().catch(console.error);
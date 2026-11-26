const amqp = require('amqplib');
const { RABBIT_URL, EXCHANGES } = require('./config');

// usage: node exchange_consumer.js <type> <label> <bindingKeys | header spec>
const [type, label, spec = ''] = process.argv.slice(2);

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    const ex = EXCHANGES[type.toUpperCase()];
    await ch.assertExchange(ex, type, { durable: true });

    // temporary private queue, auto-deleted when this consumer disconnects
    const { queue } = await ch.assertQueue('', { exclusive: true });

    if (type === 'headers') {
        const args = Object.fromEntries(spec.split(',').map((p) => p.split('=')));
        await ch.bindQueue(queue, ex, '', args);
    } else if (type === 'fanout') {
        await ch.bindQueue(queue, ex, '');
    } else {
        for (const key of spec.split(',')) await ch.bindQueue(queue, ex, key);
    }

    console.log(`[${label}] ${type} bound: ${spec || '(all)'}`);
    ch.consume(queue, (m) => {
        if (!m) return;
        console.log(`[${label}] key="${m.fields.routingKey}" msg=${m.content.toString()}`);
        ch.ack(m);
    });
}

main().catch(console.error);
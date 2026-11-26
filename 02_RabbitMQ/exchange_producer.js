const amqp = require('amqplib');
const { RABBIT_URL, EXCHANGES } = require('./config');

// usage: node exchange_producer.js <type> <routingKey | header spec>
const [type, key = ''] = process.argv.slice(2);

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    const ex = EXCHANGES[type.toUpperCase()];
    await ch.assertExchange(ex, type, { durable: true });

    const body = Buffer.from(`${type}:${key}`);
    if (type === 'headers') {
        const headers = Object.fromEntries(key.split(',').map((p) => p.split('=')));
        ch.publish(ex, '', body, { headers });
    } else {
        ch.publish(ex, key, body);
    }
    console.log(`Published ${type}:${key}`);

    await ch.close();
    await conn.close();
}

main().catch(console.error);
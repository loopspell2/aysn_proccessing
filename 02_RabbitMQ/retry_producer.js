const amqp = require('amqplib');
const { RABBIT_URL, QUEUES, EXCHANGES } = require('./config');
const setup = require('./retry_setup');

async function main() {
    const mode = process.argv[2];
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await setup(ch);
    const send = (key, obj, opts = {}) =>
        ch.publish(EXCHANGES.RETRY, key, Buffer.from(JSON.stringify(obj)), { persistent: true, ...opts });

    if (mode === 'delay') {
        send('wait', { id: 9, type: 'ok' }); // goes to wait queue, arrives in main after 3s
        console.log('Sent delayed message #9 at', new Date().toISOString());
    } else if (mode === 'ttl') {
        await ch.assertQueue(QUEUES.TTL_DEMO, { durable: true });
        ch.sendToQueue(QUEUES.TTL_DEMO, Buffer.from('expires soon'), { expiration: '5000' });
        console.log('Sent message with 5s TTL, no consumer');
    } else {
        send('main', { id: 1, type: 'ok' });
        send('main', { id: 2, type: 'flaky' });       // fails twice, then works
        send('main', { id: 3, type: 'always_fail' }); // never works
        console.log('Sent 3 tasks');
    }
    await ch.close();
    await conn.close();
}

main().catch(console.error);
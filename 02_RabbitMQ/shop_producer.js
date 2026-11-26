const { QUEUES, EXCHANGES } = require('./config');
const { connectWithRetry } = require('./connection');
const setup = require('./shop_setup');

async function main() {
    const conn = await connectWithRetry('producer');
    const ch = await conn.createConfirmChannel();
    await setup(ch);
    ch.on('return', (m) => console.log('UNROUTABLE:', m.fields.routingKey));

    const publish = (key, order) =>
        ch.publish(EXCHANGES.SHOP, key, Buffer.from(JSON.stringify(order)), {
            persistent: true,
            mandatory: true,
            messageId: `${key}-${order.id}`, // same business event = same id
        });

    publish('order.created', { id: 1001 });
    publish('order.created', { id: 1002, fail: 'flaky' });  // inventory fails twice, then works
    publish('order.created', { id: 1003, fail: 'always' }); // email never works
    publish('order.created', { id: 1001 });                 // duplicate
    publish('order.cancelled', { id: 1004 });               // email only
    await ch.waitForConfirms();
    console.log('All 5 messages confirmed by broker');

    await ch.close();
    await conn.close();
}

main().catch((e) => { console.error(e.message); process.exit(1); });
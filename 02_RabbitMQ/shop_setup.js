const { QUEUES, EXCHANGES, RETRY_DELAY_MS } = require('./config');

const BINDINGS = [
    { queue: QUEUES.SHOP_INVENTORY, key: 'order.created' },
    { queue: QUEUES.SHOP_EMAIL, key: 'order.*' },
];

module.exports = async function setup(ch) {
    await ch.assertExchange(EXCHANGES.SHOP, 'topic', { durable: true });
    await ch.assertExchange(EXCHANGES.SHOP_DLX, 'direct', { durable: true });
    await ch.assertQueue(QUEUES.SHOP_DLQ, { durable: true });
    await ch.bindQueue(QUEUES.SHOP_DLQ, EXCHANGES.SHOP_DLX, 'dead');

    for (const { queue, key } of BINDINGS) {
        // main queue: rejected messages go to the DLQ
        await ch.assertQueue(queue, {
            durable: true,
            arguments: { 'x-dead-letter-exchange': EXCHANGES.SHOP_DLX, 'x-dead-letter-routing-key': 'dead' },
        });
        await ch.bindQueue(queue, EXCHANGES.SHOP, key);

        // wait queue: after the TTL, message returns to the main queue via the default exchange
        await ch.assertQueue(`${queue}.wait`, {
            durable: true,
            arguments: {
                'x-message-ttl': RETRY_DELAY_MS,
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': queue,
            },
        });
    }
};
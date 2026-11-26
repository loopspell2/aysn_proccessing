const { QUEUES, EXCHANGES } = require('./config');

module.exports = async function setup(ch) {
    await ch.assertExchange(EXCHANGES.DLX, 'direct', { durable: true });
    await ch.assertQueue(QUEUES.DLQ, { durable: true });
    await ch.bindQueue(QUEUES.DLQ, EXCHANGES.DLX, 'dead');

    const dlxArgs = {
        'x-dead-letter-exchange': EXCHANGES.DLX,
        'x-dead-letter-routing-key': 'dead',
    };
    await ch.assertQueue(QUEUES.ORDERS, { durable: true, arguments: dlxArgs });
    await ch.assertQueue(QUEUES.LIMITED, {
        durable: true,
        arguments: { ...dlxArgs, 'x-max-length': 2 }, // keep only 2 messages
    });
};
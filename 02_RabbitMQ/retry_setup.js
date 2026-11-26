const { QUEUES, EXCHANGES, RETRY_DELAY_MS } = require('./config');

module.exports = async function setup(ch) {
    const ex = EXCHANGES.RETRY;
    await ch.assertExchange(ex, 'direct', { durable: true });

    await ch.assertQueue(QUEUES.RETRY_MAIN, { durable: true });
    await ch.bindQueue(QUEUES.RETRY_MAIN, ex, 'main');

    await ch.assertQueue(QUEUES.RETRY_WAIT, {
        durable: true,
        arguments: {
            'x-message-ttl': RETRY_DELAY_MS,       // expire after 3s
            'x-dead-letter-exchange': ex,          // then send to this exchange
            'x-dead-letter-routing-key': 'main',   // with this key -> main queue
        },
    });
    await ch.bindQueue(QUEUES.RETRY_WAIT, ex, 'wait');
};
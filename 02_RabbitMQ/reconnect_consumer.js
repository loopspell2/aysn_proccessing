const { QUEUES } = require('./config');
const { connectWithRetry } = require('./connection');

async function start() {
    const conn = await connectWithRetry('consumer');

    conn.on('error', (e) => console.log('[consumer] connection error:', e.message));
    conn.on('close', () => {
        console.log('[consumer] connection closed, reconnecting in 1s...');
        setTimeout(start, 1000);
    });

    const ch = await conn.createChannel();
    ch.on('error', (e) => console.log('[consumer] channel error:', e.message));
    ch.on('close', () => {
        // channel died but connection may be alive: close it to trigger a full restart
        conn.close().catch(() => { });
    });

    await ch.assertQueue(QUEUES.RECONNECT, { durable: true });
    await ch.prefetch(5);
    console.log('[consumer] waiting...');

    ch.consume(QUEUES.RECONNECT, (m) => {
        if (!m) return; // broker cancelled the consumer
        console.log('[consumer] got', m.content.toString());
        ch.ack(m);
    });
}

start().catch(console.error);
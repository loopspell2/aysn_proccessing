const { QUEUES } = require('./config');
const { connectWithRetry } = require('./connection');

let ch = null;
const pending = []; // in-memory buffer while disconnected
let n = 0;

function send(msg) {
    ch.sendToQueue(QUEUES.RECONNECT, Buffer.from(JSON.stringify(msg)), { persistent: true });
}

async function connect() {
    const conn = await connectWithRetry('producer');

    conn.on('error', (e) => console.log('[producer] connection error:', e.message));
    conn.on('close', () => {
        ch = null;
        console.log('[producer] connection closed, reconnecting in 1s...');
        setTimeout(connect, 1000);
    });

    const c = await conn.createChannel();
    c.on('error', (e) => console.log('[producer] channel error:', e.message));
    await c.assertQueue(QUEUES.RECONNECT, { durable: true });
    ch = c;

    while (pending.length) send(pending.shift()); // flush what we buffered
    console.log('[producer] ready');
}

setInterval(() => {
    const msg = { n: ++n };
    try {
        if (!ch) throw new Error('no channel');
        send(msg);
        console.log('[producer] sent', n);
    } catch {
        pending.push(msg);
        console.log('[producer] buffered', n);
    }
}, 2000);

connect().catch(console.error);
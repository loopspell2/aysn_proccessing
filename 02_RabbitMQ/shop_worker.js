const { QUEUES, MAX_ATTEMPTS } = require('./config');
const { connectWithRetry, sleep } = require('./connection');
const setup = require('./shop_setup');

const service = process.argv[2]; // inventory | email
const SERVICES = {
    inventory: {
        queue: QUEUES.SHOP_INVENTORY,
        run: (o, n) => {
            if (o.fail === 'flaky' && n < 3) throw new Error('stock service timeout');
            console.log(`  inventory: reserved stock for order ${o.id}`);
        },
    },
    email: {
        queue: QUEUES.SHOP_EMAIL,
        run: (o) => {
            if (o.fail === 'always') throw new Error('mail server rejected');
            console.log(`  email: sent mail for order ${o.id}`);
        },
    },
};
const svc = SERVICES[service];
if (!svc) { console.log('usage: node shop_worker.js inventory|email'); process.exit(1); }

const processed = new Set(); // use Redis or a DB with a TTL in production
const log = (s) => console.log(`[${service}] ${s}`);

async function main() {
    const conn = await connectWithRetry(service);
    let shuttingDown = false;
    conn.on('error', (e) => log(`connection error: ${e.message}`));
    conn.on('close', () => {
        if (shuttingDown) return;
        log('connection lost, exiting so a process manager can restart us');
        process.exit(1);
    });

    const ch = await conn.createChannel();
    await setup(ch);
    await ch.prefetch(1); // one at a time, so the duplicate check has no race

    let inFlight = 0;
    const { consumerTag } = await ch.consume(svc.queue, async (m) => {
        if (!m) return;
        inFlight++;
        try {
            const id = m.properties.messageId;
            const order = JSON.parse(m.content.toString());
            const n = ((m.properties.headers || {}).attempts || 0) + 1;

            if (processed.has(id)) {
                log(`SKIP duplicate ${id}`);
                return ch.ack(m);
            }
            try {
                await sleep(300); // simulate work
                svc.run(order, n);
                processed.add(id);
                ch.ack(m);
                log(`OK ${id} (attempt ${n})`);
            } catch (err) {
                if (n >= MAX_ATTEMPTS) {
                    ch.nack(m, false, false); // goes to shop_dlq
                    log(`DEAD ${id} after ${n} attempts (${err.message})`);
                } else {
                    ch.sendToQueue(`${svc.queue}.wait`, m.content, {
                        persistent: true,
                        messageId: id,
                        headers: { attempts: n },
                    });
                    ch.ack(m);
                    log(`FAIL ${id} attempt ${n} (${err.message}), retry in 3s`);
                }
            }
        } finally {
            inFlight--;
        }
    });
    log('waiting for orders...');

    async function shutdown(sig) {
        if (shuttingDown) return;
        shuttingDown = true;
        log(`${sig}: draining ${inFlight} in-flight`);
        setTimeout(() => process.exit(1), 10000).unref();
        await ch.cancel(consumerTag);
        while (inFlight > 0) await sleep(100);
        await ch.close();
        await conn.close();
        log('clean exit');
        process.exit(0);
    }
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((e) => { console.error(e.message); process.exit(1); });
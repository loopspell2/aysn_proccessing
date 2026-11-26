const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const FORCE_EXIT_MS = 10000; // hard limit so we never hang forever

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.SHUTDOWN, { durable: true });
    await ch.prefetch(2);

    let inFlight = 0;
    let shuttingDown = false;

    const { consumerTag } = await ch.consume(QUEUES.SHUTDOWN, async (m) => {
        if (!m) return;
        inFlight++;
        const { id } = JSON.parse(m.content.toString());
        console.log(`start #${id}`);
        await sleep(3000); // simulate slow work
        ch.ack(m);
        inFlight--;
        console.log(`done  #${id}`);
    });

    console.log('Waiting... press Ctrl+C while messages are processing');

    async function shutdown(signal) {
        if (shuttingDown) return;
        shuttingDown = true;
        console.log(`\n${signal} received: stop consuming, waiting for ${inFlight} in-flight`);

        const force = setTimeout(() => {
            console.log('Timeout, forcing exit (unacked messages will be redelivered)');
            process.exit(1);
        }, FORCE_EXIT_MS);
        await ch.cancel(consumerTag);          // 1. no new messages
        while (inFlight > 0) await sleep(100); // 2. finish current work
        await ch.close();                      // 3. close channel, then connection
        await conn.close();
        clearTimeout(force);
        console.log('Clean exit');
        process.exit(0);
    }

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch(console.error);
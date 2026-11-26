const amqp = require('amqplib');
const { RABBIT_URL, QUEUES, EXCHANGES, MAX_ATTEMPTS } = require('./config');
const setup = require('./retry_setup');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await setup(ch);
    await ch.prefetch(5);
    console.log('Waiting...');

    ch.consume(QUEUES.RETRY_MAIN, (m) => {
        if (!m) return;
        const task = JSON.parse(m.content.toString());
        const n = ((m.properties.headers && m.properties.headers.attempts) || 0) + 1;
        const t = new Date().toISOString().slice(17, 23);

        try {
            if (task.type === 'flaky' && n < 3) throw new Error('temporary');
            if (task.type === 'always_fail') throw new Error('permanent');
            console.log(`[${t}] #${task.id} OK on attempt ${n}`);
        } catch (err) {
            if (n >= MAX_ATTEMPTS) {
                console.log(`[${t}] #${task.id} GIVE UP after ${n} attempts`); // Step 9: send to a DLQ instead
            } else {
                ch.publish(EXCHANGES.RETRY, 'wait', m.content, { persistent: true, headers: { attempts: n } });
                console.log(`[${t}] #${task.id} failed attempt ${n}, retry in 3s`);
            }
        }
        ch.ack(m); // always ack the original; the retry is a new message
    });
}

main().catch(console.error);
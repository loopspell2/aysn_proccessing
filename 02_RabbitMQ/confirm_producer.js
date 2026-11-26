const amqp = require('amqplib');
const { RABBIT_URL, QUEUES, EXCHANGES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createConfirmChannel(); // confirm mode

    await ch.assertQueue(QUEUES.CONFIRM, { durable: true });
    await ch.assertExchange(EXCHANGES.CONFIRM_DIRECT, 'direct', { durable: true });
    await ch.bindQueue(QUEUES.CONFIRM, EXCHANGES.CONFIRM_DIRECT, 'ok');

    // called when a mandatory message matches no queue
    ch.on('return', (msg) => {
        console.log(`RETURNED (unroutable): key="${msg.fields.routingKey}" body=${msg.content.toString()}`);
    });

    // 1. callback style: one confirm per message
    await new Promise((resolve, reject) => {
        ch.publish(EXCHANGES.CONFIRM_DIRECT, 'ok', Buffer.from('msg-1'), { persistent: true, mandatory: true }, (err) => {
            if (err) { console.log('msg-1 NACKED by broker'); return reject(err); }
            console.log('msg-1 CONFIRMED by broker');
            resolve();
        });
    });

    // 2. batch style: publish many, then wait once
    for (let i = 2; i <= 6; i++) {
        ch.publish(EXCHANGES.CONFIRM_DIRECT, 'ok', Buffer.from(`msg-${i}`), { persistent: true, mandatory: true });
    }
    await ch.waitForConfirms();
    console.log('msgs 2-6 CONFIRMED (batch)');

    // 3. unroutable message: no binding for key "nobody"
    ch.publish(EXCHANGES.CONFIRM_DIRECT, 'nobody', Buffer.from('lost-msg'), { persistent: true, mandatory: true });
    await ch.waitForConfirms();
    console.log('lost-msg was confirmed too (confirm != routed!)');

    await ch.close();
    await conn.close();
}

main().catch((e) => { console.error('FAILED:', e.message); process.exit(1); });
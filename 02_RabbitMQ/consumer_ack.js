const amqp = require('amqplib');
const { RABBIT_URL, QUEUES } = require('./config');

async function main() {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertQueue(QUEUES.TASKS);
    console.log('Waiting for tasks...');

    ch.consume(QUEUES.TASKS, (m) => {
        if (!m) return;
        const task = JSON.parse(m.content.toString());
        const redelivered = m.fields.redelivered; // true if delivered before
        console.log(`Got #${task.id} type=${task.type} redelivered=${redelivered}`);

        try {
            if (task.type === 'crash' && !redelivered) process.exit(1); // die without ack
            if (task.type === 'fail_once' && !redelivered) throw new Error('temporary failure');
            if (task.type === 'poison') throw new Error('bad data');

            ch.ack(m);
            console.log(`  -> ACK #${task.id}`);
        } catch (err) {
            const requeue = !redelivered; // retry once, then drop
            ch.nack(m, false, requeue);
            console.log(`  -> NACK #${task.id} (${err.message}) requeue=${requeue}`);
        }
    }, { noAck: false });
}

main().catch(console.error);
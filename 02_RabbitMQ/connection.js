const amqp = require('amqplib');
const { RABBIT_URL } = require('./config');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// retries forever, delay doubles up to 10s (exponential backoff)
async function connectWithRetry(label) {
    let delay = 1000;
    while (true) {
        try {
            const conn = await amqp.connect(RABBIT_URL);
            console.log(`[${label}] connected`);
            return conn;
        } catch (err) {
            console.log(`[${label}] connect failed (${err.code || err.message}), retry in ${delay}ms`);
            await sleep(delay);
            delay = Math.min(delay * 2, 10000);
        }
    }
}

module.exports = { connectWithRetry, sleep };
const connection = {
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: Number(process.env.REDIS_PORT) || 6379,
};

const QUEUE_NAME = "signup-queue";

const defaultJobOptions = {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },
    removeOnComplete: { age: 3600, count: 100 },
    removeOnFail: { age: 86400 },
};

module.exports = { connection, QUEUE_NAME, defaultJobOptions };
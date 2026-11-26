const { Queue } = require("bullmq");
const queue = new Queue("email-queue", { connection: { host: "127.0.0.1", port: 6379 } });

(async () => {
    const failed = await queue.getFailed();
    for (const job of failed) await job.retry();
    console.log(`Retried ${failed.length} jobs`);
    await queue.close();
})();
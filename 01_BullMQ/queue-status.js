const { Queue } = require("bullmq");
const queue = new Queue("email-queue", {
    connection: { host: "127.0.0.1", port: 6379 },
});

(async () => {
    const counts = await queue.getJobCounts(
        "waiting", "active", "completed", "failed", "delayed"
    );
    console.log(counts);
    await queue.close();
})();
const { Queue } = require("bullmq");
const queue = new Queue("email-queue", {
    connection: { host: "127.0.0.1", port: 6379 },
});

(async () => {
    const jobs = Array.from({ length: 10 }, (_, i) => ({
        name: "send-email",
        data: { to: `user${i + 1}@example.com`, subject: "Hello" },
    }));

    await queue.addBulk(jobs);
    console.log("10 jobs added at", new Date().toLocaleTimeString());
    await queue.close();
})();
const { Queue } = require("bullmq");

const emailQueue = new Queue("email-queue", {
    connection: { host: "127.0.0.1", port: 6379 },
});

async function main() {
    const job = await emailQueue.add(
        "send-welcome-email",
        { to: "arpit@example.com", subject: "Welcome!" },
        {
            attempts: 3,                                   // try up to 3 times
            backoff: { type: "exponential", delay: 1000 }, // wait 1s, 2s, 4s...
            removeOnComplete: { age: 60, count: 5 }, // keep max 5 completed jobs, or those younger than 60s
            removeOnFail: { age: 86400 },            // keep failed jobs for 24 hours
        }
    );
    console.log("Job added with id:", job.id);
    await emailQueue.close();
}

main();
const { Worker, Queue } = require("bullmq");

const connection = { host: "127.0.0.1", port: 6379 };
const deadQueue = new Queue("email-dead-letter", { connection });

const worker = new Worker(
    "email-queue",
    async (job) => {
        console.log(`Attempt number: ${job.attemptsMade + 1}`);
        // simulate 2 attempts fail
        // if (job.attemptsMade < 2) throw new Error("Email service down!");

        // simulate all attempts fails
        // throw new Error("Email service down!");

        console.log(`Processing job ${job.id}: ${job.name}`);
        // console.log("Data:", job.data);

        // simulate slow email sending
        // await new Promise((resolve) => setTimeout(resolve, 2000));

        // console.log(`Email sent to ${job.data.to}`);

        // when we have to update progress or moniter worker
        for (let i = 1; i <= 4; i++) {
            await new Promise((resolve) => setTimeout(resolve, 500));
            await job.updateProgress(i * 25); // 25, 50, 75, 100
        }
        return { sent: true };
    },
    {
        connection,
        concurrency: 5, // process 5 jobs at the same time
        limiter: { max: 2, duration: 1000 }, // uncomment for Part B
    }
);

worker.on("completed", (job) => console.log(`Job ${job.id} completed`));

worker.on("progress", (job, progress) => {
    console.log(`Job ${job.id} progress: ${progress}%`);
});

worker.on("failed", async (job, err) => {
    console.log(`Attempt ${job.attemptsMade}/${job.opts.attempts} failed: ${err.message}`);

    if (job.attemptsMade >= job.opts.attempts) {
        console.log(`Job ${job.id} permanently failed. Moving to dead letter queue.`);
        await deadQueue.add(
            "dead-email",
            {
                originalData: job.data,
                reason: err.message,
                failedAt: new Date().toISOString(),
            },
            { jobId: `dead-${job.id}` }
        );
    }
});

async function shutdown(signal) {
    console.log(`${signal} received. Closing worker...`);
    await worker.close();      // waits for active jobs to finish
    await deadQueue.close();
    console.log("Worker closed cleanly");
    process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));   // Ctrl+C
process.on("SIGTERM", () => shutdown("SIGTERM")); // Docker / Kubernetes / PM2
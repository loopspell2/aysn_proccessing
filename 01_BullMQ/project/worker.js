const { Worker } = require("bullmq");
const { connection, QUEUE_NAME } = require("./config");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const worker = new Worker(
    QUEUE_NAME,
    async (job) => {
        const prev = Object.values(await job.getChildrenValues())[0];
        console.log(`[${job.name}] started (attempt ${job.attemptsMade + 1})`);

        if (job.name === "validate") {
            await sleep(1000);
            if (!job.data.email.includes("@")) throw new Error("Invalid email");
            return { email: job.data.email, name: job.data.name };
        }

        if (job.name === "generate-pdf") {
            await job.updateProgress({ step: 2, total: 3, message: "Generating PDF" });
            await sleep(2000);
            return { pdfUrl: `/files/welcome-${prev.name}.pdf`, email: prev.email };
        }

        if (job.name === "send-email") {
            await job.updateProgress({ step: 3, total: 3, message: "Sending email" });
            await sleep(1000);
            console.log(`Email sent to ${prev.email} with ${prev.pdfUrl}`);
            return { sent: true };
        }
    },
    { connection, concurrency: 5 }
);

worker.on("completed", (job) => console.log(`[${job.name}] completed`));
worker.on("failed", (job, err) =>
    console.log(`[${job?.name}] failed (${job?.attemptsMade}/${job?.opts.attempts}): ${err.message}`)
);

async function shutdown(signal) {
    console.log(`${signal} received. Closing worker...`);
    await worker.close();
    console.log("Worker closed cleanly");
    process.exit(0);
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
const { Queue } = require("bullmq");
const queue = new Queue("email-queue", {
    connection: { host: "127.0.0.1", port: 6379 },
});

(async () => {
    await queue.upsertJobScheduler(
        "daily-report",                 // unique scheduler id
        { every: 5000 },                // every 5 seconds (for testing)
        {
            name: "send-report",
            data: { to: "arpit@example.com", subject: "Report" },
        }
    );
    console.log("Repeating job scheduled");
    await queue.close();
})();
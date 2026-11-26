const { Queue } = require("bullmq");
const queue = new Queue("email-queue", {
    connection: { host: "127.0.0.1", port: 6379 },
});

(async () => {
    const data = { to: "arpit@example.com", subject: "Welcome!" };
    const opts = { jobId: "welcome-arpit@example.com" }; // unique business key

    const a = await queue.add("send-welcome-email", data, opts);
    const b = await queue.add("send-welcome-email", data, opts);

    console.log("First id:", a.id);
    console.log("Second id:", b.id);

    console.log(await queue.getJobCounts("waiting", "completed"));
    await queue.close();
})();
const { Queue } = require("bullmq");
const queue = new Queue("email-queue", {
    connection: { host: "127.0.0.1", port: 6379 },
});

(async () => {
    await queue.add(
        "send-reminder",
        { to: "arpit@example.com", subject: "Reminder!" },
        { delay: 10000 } // run after 10 seconds
    );
    console.log("Delayed job added at", new Date().toLocaleTimeString());
    await queue.close();
})();
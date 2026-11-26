const express = require("express");
const { Queue } = require("bullmq");
const { createBullBoard } = require("@bull-board/api");
const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { ExpressAdapter } = require("@bull-board/express");

const connection = { host: "127.0.0.1", port: 6379 };

const emailQueue = new Queue("email-queue", { connection });
const deadQueue = new Queue("email-dead-letter", { connection });

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");

createBullBoard({
    queues: [new BullMQAdapter(emailQueue), new BullMQAdapter(deadQueue)],
    serverAdapter,
});

const app = express();
app.use("/admin/queues", serverAdapter.getRouter());

app.listen(3000, () => {
    console.log("Dashboard: http://localhost:3000/admin/queues");
});
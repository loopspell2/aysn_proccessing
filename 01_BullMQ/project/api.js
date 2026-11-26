const express = require("express");
const { FlowProducer, Queue } = require("bullmq");
const { createBullBoard } = require("@bull-board/api");
const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { ExpressAdapter } = require("@bull-board/express");
const { connection, QUEUE_NAME, defaultJobOptions } = require("./config");

const flow = new FlowProducer({ connection });
const queue = new Queue(QUEUE_NAME, { connection });

// dashboard
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");
createBullBoard({ queues: [new BullMQAdapter(queue)], serverAdapter });

const app = express();
app.use(express.json());
app.use("/admin/queues", serverAdapter.getRouter());

app.post("/signup", async (req, res) => {
    const { email, name } = req.body;
    if (!email || !name) {
        return res.status(400).json({ error: "email and name are required" });
    }

    const data = { email, name };
    const base = { queueName: QUEUE_NAME, data, opts: defaultJobOptions };

    // jobId on the root = dedupe: same email cannot start a second signup flow
    const tree = await flow.add({
        ...base,
        name: "send-email",
        opts: { ...defaultJobOptions, jobId: `signup-${email}` },
        children: [
            {
                ...base,
                name: "generate-pdf",
                opts: { ...defaultJobOptions, failParentOnFailure: true },
                children: [
                    {
                        ...base,
                        name: "validate",
                        opts: { ...defaultJobOptions, failParentOnFailure: true },
                    },
                ],
            },
        ],
    });

    res.status(202).json({ message: "Signup accepted", jobId: tree.job.id });
});

app.get("/status/:id", async (req, res) => {
    const job = await queue.getJob(req.params.id);
    if (!job) return res.status(404).json({ error: "Job not found" });
    res.json({ id: job.id, name: job.name, state: await job.getState() });
});

app.listen(3000, () => {
    console.log("API:       http://localhost:3000");
    console.log("Dashboard: http://localhost:3000/admin/queues");
});
const { MGMT, MONITOR } = require('./config');

const auth = 'Basic ' + Buffer.from(`${MGMT.USER}:${MGMT.PASS}`).toString('base64');
const get = async (p) => {
    const r = await fetch(MGMT.URL + p, { headers: { Authorization: auth } });
    if (!r.ok) throw new Error(`HTTP ${r.status} on ${p}`);
    return r.json();
};

async function check() {
    const queues = await get('/api/queues');
    const alarms = await get('/api/health/checks/alarms');
    const alerts = [];

    const rows = queues.map((q) => {
        const ready = q.messages_ready ?? 0;
        const unacked = q.messages_unacknowledged ?? 0;
        const consumers = q.consumers ?? 0;

        if (q.name.endsWith('dlq') && ready > 0) alerts.push(`DLQ ${q.name} has ${ready} dead message(s)`);
        if (ready > 0 && consumers === 0 && !q.name.endsWith('dlq') && !q.name.includes('wait'))
            alerts.push(`${q.name}: ${ready} ready but NO consumers`);
        if (ready > MONITOR.MAX_READY && consumers > 0) alerts.push(`${q.name}: backlog ${ready}`);
        if (unacked > MONITOR.MAX_UNACKED) alerts.push(`${q.name}: ${unacked} unacked`);
        return { name: q.name, ready, unacked, consumers };
    });

    if (alarms.status !== 'ok') alerts.push('Broker alarm: memory or disk limit reached');

    console.log('\n' + new Date().toISOString().slice(11, 19));
    console.table(rows);
    alerts.length ? alerts.forEach((a) => console.log('ALERT:', a)) : console.log('All OK');
}

const run = () => check().catch((e) => console.log('monitor error:', e.message));
run();
if (process.argv[2] !== 'once') setInterval(run, MONITOR.INTERVAL_MS);
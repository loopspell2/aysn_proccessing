1. we can change the durablilty of queue later ?
2. Several consumers on the same queue - They compete (work distribution, Step 6). To have every consumer receive a copy, give each its own queue, as we did here . so we have to use queue in place of exchange ?
3. Per-message TTL blocks behind the head	A queue only expires messages at the head. If a 60s message sits in front of a 5s one, the 5s one waits. Don't mix different delays in one queue; use one wait queue per delay tier (5s, 30s, 5m) ?
4. 
# Known Failure Modes

- If the app is deployed with more than one web process, the in-memory rate limiter is per process. Two processes can jointly exceed PseudoGram's 10-per-60-second limit even though each process behaves correctly by itself.

- SQLite must live on a persistent disk. If the deployment uses an ephemeral filesystem and the container restarts, queued events, delivery jobs, and stats can disappear.

- A `comment.deleted` event only cancels jobs that have not started sending yet. If deletion arrives while a job is already in the `sending` state, the DM request may still reach PseudoGram.

- If the process crashes after PseudoGram accepts a DM but before the app stores the returned `dm_id`, recovery depends on PseudoGram honoring the same `Idempotency-Key` on retry. If that idempotency record is unavailable, the app could create a second DM attempt for the same delivery job.

- If PseudoGram's status endpoint keeps returning non-200 responses forever after a `202 Accepted`, the job remains counted as `queued` indefinitely. That avoids silently marking it sent, but it delays final `sent` or `failed` accounting.

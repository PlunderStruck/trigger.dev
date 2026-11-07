# Resumed run loses its queue position when its restore fails

Reproduction for the linked issue. Not intended to be merged.

`internal-packages/run-engine/src/engine/tests/resumedRunQueuePositionRepro.test.ts` uses
`containerTest` and a real `RunEngine`. Both tests assert today's behaviour, so they pass on `main`.

```bash
cd internal-packages/run-engine
pnpm run test ./src/engine/tests/resumedRunQueuePositionRepro.test.ts --run
```

The queue has a concurrency limit of 1. A parent run is checkpointed in a wait while a run
triggered after it waits in the queue. When the wait completes, the parent's resume is dequeued
first, as `continueRunIfUnblocked` intends ("Re-enqueue with the original timestamp, so waiting
runs dequeue before new runs"). The restore then fails before the run continues:

- The restored runner never calls `continueRunExecution`, so the `PENDING_EXECUTING` heartbeat
  stalls and the engine requeues the run.
- The supervisor reports the failed restore with `reportRestoreOutcome({ outcome: "requeue" })`,
  as it does for a restore Runner that failed, for example with `OOMKilled`.

Both paths go through `tryNackAndRequeue`, which nacks the message without a retry time, so
`RunQueue` scores it at now plus backoff. The next dequeue returns the newer run, and the parent
is dequeued only after it. With a backlog, the parent waits behind every run triggered after it.

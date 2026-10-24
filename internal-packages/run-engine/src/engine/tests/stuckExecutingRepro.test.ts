// Reproduces a run that stays EXECUTING after its attempt completion is lost.
// Each test asserts today's behaviour, so the file passes on main and documents the bug.
import { containerTest, assertNonNullable } from "@internal/testcontainers";
import { trace } from "@internal/tracing";
import { setTimeout } from "timers/promises";
import { RunEngine } from "../index.js";
import { setupAuthenticatedEnvironment, setupBackgroundWorker } from "./setup.js";

vi.setConfig({ testTimeout: 60_000 });

const taskIdentifier = "test-task";

function createEngine(
  prisma: ConstructorParameters<typeof RunEngine>[0]["prisma"],
  redisOptions: ConstructorParameters<typeof RunEngine>[0]["runLock"]["redis"],
  executingTimeout: number
) {
  return new RunEngine({
    prisma,
    worker: { redis: redisOptions, workers: 1, tasksPerWorker: 10, pollIntervalMs: 100 },
    queue: {
      redis: redisOptions,
      masterQueueConsumersDisabled: true,
      processWorkerQueueDebounceMs: 50,
    },
    runLock: {
      redis: redisOptions,
      //give up on a held lock quickly instead of after 15s
      retryConfig: { maxAttempts: 2, baseDelay: 50, maxDelay: 50, maxTotalWaitTime: 200 },
    },
    machines: {
      defaultMachine: "small-1x",
      machines: {
        "small-1x": { name: "small-1x" as const, cpu: 0.5, memory: 0.5, centsPerMs: 0.0001 },
      },
      baseCostInCents: 0.0001,
    },
    heartbeatTimeoutsMs: {
      PENDING_EXECUTING: executingTimeout,
      EXECUTING: executingTimeout,
    },
    tracer: trace.getTracer("test", "0.0.0"),
  });
}

async function triggerAndDequeue(
  engine: RunEngine,
  environment: Awaited<ReturnType<typeof setupAuthenticatedEnvironment>>,
  prisma: ConstructorParameters<typeof RunEngine>[0]["prisma"]
) {
  await setupBackgroundWorker(engine, environment, taskIdentifier);

  const run = await engine.trigger(
    {
      number: 1,
      friendlyId: "run_1234",
      environment,
      taskIdentifier,
      payload: "{}",
      payloadType: "application/json",
      context: {},
      traceContext: {},
      traceId: "t12345",
      spanId: "s12345",
      workerQueue: "main",
      queue: `task/${taskIdentifier}`,
      isTest: false,
      tags: [],
    },
    prisma
  );

  await setTimeout(500);

  const dequeued = await engine.dequeueFromWorkerQueue({
    consumerId: "test_12345",
    workerQueue: "main",
  });

  return { run, dequeued: dequeued[0] };
}

describe("Run stays EXECUTING after its attempt completion is lost", () => {
  containerTest(
    "a held run lock dead-letters the heartbeat job and the run stays EXECUTING",
    async ({ prisma, redisOptions }) => {
      const environment = await setupAuthenticatedEnvironment(prisma, "PRODUCTION");
      const engine = createEngine(prisma, redisOptions, 2_000);

      try {
        const { run, dequeued } = await triggerAndDequeue(engine, environment, prisma);
        const attempt = await engine.startRunAttempt({
          runId: run.id,
          snapshotId: dequeued.snapshot.id,
        });
        expect(attempt.snapshot.executionStatus).toBe("EXECUTING");

        const worker = (engine as any).worker;
        const jobId = `heartbeatSnapshot.${run.id}`;

        //hold the run lock the way a hung attempt completion would
        await (engine as any).runLock.lock("test-holder", [run.id], async () => {
          //the heartbeat job has already failed 11 times, so this failure exhausts the
          //default budget of 12 attempts without waiting hours of backoff
          await worker.queue.enqueue({
            id: jobId,
            job: "heartbeatSnapshot",
            item: { runId: run.id, snapshotId: attempt.snapshot.id },
            attempt: 11,
            visibilityTimeoutMs: 30_000,
          });

          //the stall handler can't get the lock, fails, and the job is dead-lettered
          await setTimeout(1_000);
          expect(await worker.queue.sizeOfDeadLetterQueue()).toBe(1);
          expect(await worker.getJob(jobId)).toBeNull();
        });

        //the lock is free again, but nothing is left to fail the run. A working stall
        //handler (2s EXECUTING timeout) would have requeued it by now
        await setTimeout(4_000);

        const executionData = await engine.getRunExecutionData({ runId: run.id });
        assertNonNullable(executionData);
        expect(executionData.snapshot.executionStatus).toBe("EXECUTING");
        expect(executionData.run.status).toBe("EXECUTING");

        //the repair tools don't touch EXECUTING runs
        const repair = await engine.repairEnvironment(environment, false);
        expect(repair.repairs.every((r) => r.action === "ignore")).toBe(true);
      } finally {
        await engine.quit();
      }
    }
  );

  containerTest(
    "a lost EXECUTING heartbeat enqueue leaves the run EXECUTING",
    async ({ prisma, redisOptions }) => {
      const environment = await setupAuthenticatedEnvironment(prisma, "PRODUCTION");
      const engine = createEngine(prisma, redisOptions, 500);

      try {
        const { run, dequeued } = await triggerAndDequeue(engine, environment, prisma);

        const worker = (engine as any).worker;
        const jobId = `heartbeatSnapshot.${run.id}`;

        //lose the heartbeat enqueue for the EXECUTING snapshot
        const originalEnqueue = worker.enqueue.bind(worker);
        let dropped = 0;
        const enqueueSpy = vi.spyOn(worker, "enqueue").mockImplementation(async (opts: any) => {
          if (opts.job === "heartbeatSnapshot" && dropped === 0) {
            dropped++;
            return;
          }
          return originalEnqueue(opts);
        });

        const attempt = await engine.startRunAttempt({
          runId: run.id,
          snapshotId: dequeued.snapshot.id,
        });
        enqueueSpy.mockRestore();
        expect(dropped).toBe(1);
        expect(attempt.snapshot.executionStatus).toBe("EXECUTING");

        //the job still points at the PENDING_EXECUTING snapshot
        expect((await worker.getJob(jobId))?.item.snapshotId).toBe(dequeued.snapshot.id);

        //runner heartbeats for the EXECUTING snapshot keep extending that stale job
        for (let i = 0; i < 4; i++) {
          await engine.heartbeatRun({ runId: run.id, snapshotId: attempt.snapshot.id });
          await setTimeout(250);
        }

        //heartbeats stop (the completion was lost); the stale job fires, sees it is
        //"no longer the latest snapshot" and stops
        await setTimeout(3_000);
        expect(await worker.getJob(jobId)).toBeNull();
        expect(await worker.queue.sizeOfDeadLetterQueue()).toBe(0);

        const executionData = await engine.getRunExecutionData({ runId: run.id });
        assertNonNullable(executionData);
        expect(executionData.snapshot.executionStatus).toBe("EXECUTING");
        expect(executionData.run.status).toBe("EXECUTING");
      } finally {
        await engine.quit();
      }
    }
  );
});

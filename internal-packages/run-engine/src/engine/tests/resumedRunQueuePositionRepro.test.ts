// Reproduces a resumed run that loses its queue position when its restore fails before the run
// continues. Each test asserts today's behaviour, so the file passes on main and documents the bug.
import { assertNonNullable, containerTest } from "@internal/testcontainers";
import { trace } from "@internal/tracing";
import type { PrismaClient } from "@trigger.dev/database";
import { setTimeout } from "node:timers/promises";
import { expect } from "vitest";
import { RunEngine } from "../index.js";
import { setupAuthenticatedEnvironment, setupBackgroundWorker } from "./setup.js";

vi.setConfig({ testTimeout: 60_000 });

const taskIdentifier = "orchestrator";
const queue = `task/${taskIdentifier}`;

type RedisOptions = ConstructorParameters<typeof RunEngine>[0]["runLock"]["redis"];
type Environment = Awaited<ReturnType<typeof setupAuthenticatedEnvironment>>;

function createEngine(
  prisma: PrismaClient,
  redisOptions: RedisOptions,
  pendingExecutingMs = 60_000
) {
  return new RunEngine({
    prisma,
    worker: { redis: redisOptions, workers: 1, tasksPerWorker: 10, pollIntervalMs: 50 },
    queue: {
      redis: redisOptions,
      masterQueueConsumersDisabled: true,
      processWorkerQueueDebounceMs: 50,
      // A short nack backoff, so the order below comes from where the requeue scores the run.
      retryOptions: { minTimeoutInMs: 10, maxTimeoutInMs: 50 },
    },
    runLock: { redis: redisOptions },
    machines: {
      defaultMachine: "small-1x",
      machines: {
        "small-1x": { name: "small-1x" as const, cpu: 0.5, memory: 0.5, centsPerMs: 0.0001 },
      },
      baseCostInCents: 0.0005,
    },
    heartbeatTimeoutsMs: { PENDING_EXECUTING: pendingExecutingMs },
    tracer: trace.getTracer("test", "0.0.0"),
  });
}

async function dequeueNext(engine: RunEngine, environmentId: string) {
  await setTimeout(300);
  await engine.runQueue.processMasterQueueForEnvironment(environmentId);
  const dequeued = await engine.dequeueFromWorkerQueue({
    consumerId: "test_consumer",
    workerQueue: "main",
  });
  expect(dequeued.length).toBe(1);
  assertNonNullable(dequeued[0]);
  return dequeued[0];
}

function triggerRun(
  engine: RunEngine,
  prisma: PrismaClient,
  environment: Environment,
  friendlyId: string
) {
  return engine.trigger(
    {
      number: 1,
      friendlyId,
      environment,
      taskIdentifier,
      payload: "{}",
      payloadType: "application/json",
      context: {},
      traceContext: {},
      traceId: `t_${friendlyId}`,
      spanId: `s_${friendlyId}`,
      workerQueue: "main",
      queue,
      isTest: false,
      tags: [],
    },
    prisma
  );
}

async function waitForExecutionStatus(engine: RunEngine, runId: string, status: string) {
  for (let i = 0; i < 100; i++) {
    const data = await engine.getRunExecutionData({ runId });
    if (data?.snapshot.executionStatus === status) {
      return data;
    }
    await setTimeout(100);
  }
  throw new Error(`run ${runId} never reached ${status}`);
}

/**
 * A parent waits with a checkpoint while a run created after it queues behind the queue's only
 * slot. The parent's wait completes and its resume is dequeued first, as designed. The restore
 * then fails before the run continues; `failRestore` is how the engine learns that.
 */
async function resumeThenFailRestore(
  engine: RunEngine,
  prisma: PrismaClient,
  failRestore: (runId: string, snapshotId: string) => Promise<void>
) {
  const environment = await setupAuthenticatedEnvironment(prisma, "PRODUCTION");
  await setupBackgroundWorker(engine, environment, taskIdentifier, undefined, undefined, {
    concurrencyLimit: 1,
  });

  const parent = await triggerRun(engine, prisma, environment, "run_parent0001");
  const firstDequeue = await dequeueNext(engine, environment.id);
  expect(firstDequeue.run.id).toBe(parent.id);
  await engine.startRunAttempt({ runId: parent.id, snapshotId: firstDequeue.snapshot.id });

  const newer = await triggerRun(engine, prisma, environment, "run_newer0001");
  expect(newer.createdAt.getTime()).toBeGreaterThan(parent.createdAt.getTime());
  await setTimeout(300);
  expect(await engine.runQueue.lengthOfQueue(environment, queue)).toBe(1);

  const { waitpoint } = await engine.createManualWaitpoint({
    environmentId: environment.id,
    projectId: environment.projectId,
  });
  const blocked = await engine.blockRunWithWaitpoint({
    runId: parent.id,
    waitpoints: waitpoint.id,
    projectId: environment.projectId,
    organizationId: environment.organizationId,
  });
  const checkpoint = await engine.createCheckpoint({
    runId: parent.id,
    snapshotId: blocked.id,
    checkpoint: {
      type: "DOCKER",
      reason: "WAIT_FOR_BATCH",
      location: "test-location",
      imageRef: "test-image-ref",
    },
  });
  expect(checkpoint.ok).toBe(true);

  await engine.completeWaitpoint({ id: waitpoint.id });
  await waitForExecutionStatus(engine, parent.id, "QUEUED");

  const restore = await dequeueNext(engine, environment.id);
  expect(restore.run.id, "the resume is dequeued before the newer run").toBe(parent.id);
  expect(restore.snapshot.executionStatus).toBe("PENDING_EXECUTING");
  assertNonNullable(restore.checkpoint);

  await failRestore(parent.id, restore.snapshot.id);

  const requeued = await waitForExecutionStatus(engine, parent.id, "QUEUED");
  expect(requeued.checkpoint?.id).toBe(restore.checkpoint.id);
  const parentRow = await prisma.taskRun.findUniqueOrThrow({ where: { id: parent.id } });
  expect(parentRow.status, "the API shows QUEUED").toBe("PENDING");
  expect(parentRow.attemptNumber, "no new attempt was recorded").toBe(1);

  const next = await dequeueNext(engine, environment.id);
  expect(next.run.id, "the run created after the parent is dequeued first").toBe(newer.id);

  const newerAttempt = await engine.startRunAttempt({
    runId: newer.id,
    snapshotId: next.snapshot.id,
  });
  await engine.completeRunAttempt({
    runId: newer.id,
    snapshotId: newerAttempt.snapshot.id,
    completion: { ok: true, id: newer.id, output: "{}", outputType: "application/json" },
  });

  const parentAgain = await dequeueNext(engine, environment.id);
  expect(parentAgain.run.id, "the parent is dequeued only after the newer run").toBe(parent.id);
  expect(parentAgain.checkpoint?.id).toBe(restore.checkpoint.id);
}

describe("A resumed run whose restore fails before it continues", () => {
  containerTest(
    "loses its queue position when the dequeued restore stalls",
    async ({ prisma, redisOptions }) => {
      const engine = createEngine(prisma, redisOptions, 500);
      try {
        // No call: the restored runner never reaches continueRunExecution, so the
        // PENDING_EXECUTING heartbeat stalls and the engine requeues the run.
        await resumeThenFailRestore(engine, prisma, async () => {});
      } finally {
        await engine.quit();
      }
    }
  );

  containerTest(
    "loses its queue position when the supervisor reports a failed restore",
    async ({ prisma, redisOptions }) => {
      const engine = createEngine(prisma, redisOptions);
      try {
        // What the supervisor reports for a restore Runner that fails, for example OOMKilled.
        await resumeThenFailRestore(engine, prisma, async (runId, snapshotId) => {
          const result = await engine.reportRestoreOutcome({
            runId,
            snapshotId,
            outcome: "requeue",
            reason: "OOMKilled",
          });
          expect(result).toEqual({ ok: true, outcome: "requeue" });
        });
      } finally {
        await engine.quit();
      }
    }
  );
});

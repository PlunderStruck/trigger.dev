// Reproduces the managed run controller's behavior when the supervisor rejects
// an attempt completion. Uses the published trigger.dev RunExecution and the
// real WorkloadHttpClient against a local fake supervisor.
//
// Usage: node repro.mjs <scenario> [observeSeconds]
//   scenario: "http500" (supervisor answers 500) or "down" (connection refused)
import http from "node:http";
import { RunExecution } from "./node_modules/trigger.dev/dist/esm/entryPoints/managed/execution.js";
import { WorkloadHttpClient } from "@trigger.dev/core/v3/runEngineWorker";

const scenario = process.argv[2] ?? "http500";
const observeSeconds = Number(process.argv[3] ?? 90);
const t0 = Date.now();
const ts = () => `+${((Date.now() - t0) / 1000).toFixed(2)}s`;
const events = [];
const record = (kind, detail) => {
  events.push({ t: ts(), kind, detail });
  console.log(ts(), kind, detail ?? "");
};

const RUN = "run_repro0001";
const SNAP_PENDING = "snapshot_pending01";
const SNAP_EXEC = "snapshot_zexec0001";

// Fake supervisor workload server.
const requests = [];
const server = http.createServer((req, res) => {
  requests.push({ t: ts(), method: req.method, url: req.url });
  record("supervisor <-", `${req.method} ${req.url}`);
  if (req.url.endsWith("/attempts/complete")) {
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "simulated engine failure" }));
    return;
  }
  res.writeHead(200, { "content-type": "application/json" });
  res.end("{}");
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;
const apiUrl = scenario === "down" ? "http://127.0.0.1:1" : `http://127.0.0.1:${port}`;

const httpClient = new WorkloadHttpClient({
  workerApiUrl: apiUrl,
  runnerId: "runner_repro",
  deploymentId: "deployment_repro",
  deploymentVersion: "20260101.1",
  projectRef: "proj_repro",
});
// Only the completion path is under test; the start, heartbeat and snapshot
// polling calls are stubbed so the fake supervisor does not need their schemas.
httpClient.startRunAttempt = async () => ({
  success: true,
  data: {
    run: { id: "r1", friendlyId: RUN, attemptNumber: 1 },
    snapshot: { id: "s2", friendlyId: SNAP_EXEC, executionStatus: "EXECUTING" },
    execution: { run: { id: RUN, traceContext: {} }, project: { ref: "proj_repro" } },
    envVars: {},
  },
});
httpClient.heartbeatRun = async (runId, snapshotId) => {
  record("heartbeat ->", `${runId} ${snapshotId}`);
  return { success: true, data: { ok: true } };
};
httpClient.getSnapshotsSince = async () => ({ success: true, data: { snapshots: [] } });

const evt = () => {
  const handlers = [];
  return { handlers, attach: (fn) => handlers.push(fn) };
};
const proc = {
  isBeingKilled: false,
  onTaskRunHeartbeat: evt(),
  onSendDebugLog: evt(),
  onSetSuspendable: evt(),
  unsafeDetachEvtHandlers() {
    this.onTaskRunHeartbeat.handlers.length = 0;
  },
  // The task child: sends heartbeats while run() executes, then succeeds.
  async execute() {
    record("task", "run() started");
    for (let i = 0; i < 3; i++) {
      await new Promise((r) => setTimeout(r, 1000));
      for (const h of proc.onTaskRunHeartbeat.handlers) await h(RUN);
    }
    record("task", "run() finished OK; child process returns completion");
    return {
      ok: true,
      id: RUN,
      output: '"done"',
      outputType: "application/json",
      usage: { durationMs: 3000 },
    };
  },
};

const execution = new RunExecution({
  workerManifest: {},
  env: {
    TRIGGER_RUNNER_ID: "runner_repro",
    TRIGGER_SNAPSHOT_POLL_INTERVAL_SECONDS: 5,
    TRIGGER_SUPERVISOR_API_URL: apiUrl,
    rawForLogging: {},
  },
  httpClient,
  logger: {
    sendDebugLog: ({ message, properties }) =>
      record("debugLog", `${message}${properties?.error ? ` :: ${properties.error}` : ""}`),
  },
  supervisorSocket: { on() {}, off() {}, emit() {}, removeAllListeners() {} },
  taskRunProcessProvider: {
    getProcess: async () => proc,
    returnProcess: async () => record("task", "child process returned/exited"),
    handleProcessAbort: async () => {},
    handleImmediateRetry: async () => {},
  },
});

await execution.execute({ runFriendlyId: RUN, snapshotFriendlyId: SNAP_PENDING });
record("controller", "RunExecution.execute() returned");

await new Promise((r) => setTimeout(r, observeSeconds * 1000));
const completions = requests.filter((r) => r.url.endsWith("/attempts/complete"));
console.log("\n=== summary ===");
console.log(
  JSON.stringify(
    {
      scenario,
      observedSecondsAfterExecuteReturned: observeSeconds,
      completionRequestsReceivedBySupervisor: completions.length,
      completionRequestTimes: completions.map((r) => r.t),
      heartbeatsSent: events.filter((e) => e.kind === "heartbeat ->").map((e) => e.t),
      lastEventTime: events.at(-1)?.t,
      finalDebugLogs: events
        .filter((e) => e.kind === "debugLog")
        .slice(-4)
        .map((e) => `${e.t} ${e.detail}`),
    },
    null,
    2
  )
);
server.close();
process.exit(0);

# Run stays EXECUTING after its attempt completion is lost

Reproduction for the linked issue. Not intended to be merged.

## 1. The managed runner gives up on the completion after about 7.5 s

`repro.mjs` drives the published `RunExecution` and the real `WorkloadHttpClient` against a
fake supervisor that fails `attempts/complete`. The task process is stubbed: it sends three
heartbeats and returns a successful completion.

```bash
mkdir /tmp/repro && cp repro.mjs /tmp/repro && cd /tmp/repro
echo '{"type":"module"}' > package.json
npm i trigger.dev@4.8.1          # same result with 4.5.14
node repro.mjs http500 120       # supervisor answers 500
node repro.mjs down 120          # supervisor unreachable
```

Result: 5 completion requests within about 7.5 s, then `failed to complete run`,
`[shutdown] execute call finished`, and no further requests or heartbeats.

## 2. Nothing ends the run on the engine side

`internal-packages/run-engine/src/engine/tests/stuckExecutingRepro.test.ts` uses
`containerTest` and a real `RunEngine`. Both tests assert today's behaviour, so they pass on `main`.

```bash
cd internal-packages/run-engine
pnpm run test ./src/engine/tests/stuckExecutingRepro.test.ts --run
```

- A held run lock makes the `heartbeatSnapshot` job fail until it is dead-lettered. The run
  then stays EXECUTING, and `repairEnvironment` ignores it.
- A lost heartbeat enqueue for the EXECUTING snapshot leaves the older job in place. Runner
  heartbeats extend it, and once they stop it finds it is "no longer the latest snapshot" and
  stops. The run stays EXECUTING.

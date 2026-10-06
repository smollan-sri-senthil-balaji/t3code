import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  JetskiSettings,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "../IdAllocator.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import { makeJetskiAdapterV2 } from "./JetskiAdapterV2.ts";
import type { JetskiRecord } from "./JetskiCli.ts";

const layerServerConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-jetski-v2-adapter-",
}).pipe(Layer.provide(NodeServices.layer));

const layerTest = Layer.mergeAll(NodeServices.layer, IdAllocator.layer, layerServerConfig);

const decodeJsonLine = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const JETSKI_INSTANCE_ID = ProviderInstanceId.make("jetski");
const THREAD_ID = ThreadId.make("thread-jetski-test");
const SESSION_ID = ProviderSessionId.make("provider-session-jetski-test");
/** Outside the valid pid range, so the adapter's tree kill never lands. */
const FAKE_PID = 999_999_999;

const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
});

const modelSelection: ModelSelection = { instanceId: JETSKI_INSTANCE_ID, model: "default" };

const jetskiSettings = Schema.decodeSync(JetskiSettings)({ binaryPath: "jetski-cli-fake" });

/** In-process fake jetski-cli: records stdin events and lets tests write stdout events. */
const makeFakeJetski = Effect.gen(function* () {
  const stdout = yield* Queue.unbounded<Uint8Array, Cause.Done>();
  const inputs = yield* Queue.unbounded<JetskiRecord>();
  let spawns = 0;
  let stdinBuffer = "";

  const handleStdinChunk = (chunk: Uint8Array) =>
    Effect.gen(function* () {
      stdinBuffer += new TextDecoder().decode(chunk);
      let newline = stdinBuffer.indexOf("\n");
      while (newline !== -1) {
        const line = stdinBuffer.slice(0, newline);
        stdinBuffer = stdinBuffer.slice(newline + 1);
        if (line.length > 0) yield* Queue.offer(inputs, decodeJsonLine(line) as JetskiRecord);
        newline = stdinBuffer.indexOf("\n");
      }
    });

  const spawner = ChildProcessSpawner.make(() =>
    Effect.sync(() => {
      spawns += 1;
      return ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(FAKE_PID),
        exitCode: Effect.never,
        isRunning: Effect.succeed(true),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.forEach(handleStdinChunk),
        stdout: Stream.fromQueue(stdout),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      });
    }),
  );

  return {
    spawner,
    emit: (record: JetskiRecord) =>
      Queue.offer(stdout, new TextEncoder().encode(`${encodeJsonLine(record)}\n`)).pipe(
        Effect.asVoid,
      ),
    takeInput: Queue.take(inputs),
    spawnCount: () => spawns,
  };
});

type FakeJetski = Effect.Success<typeof makeFakeJetski>;

const openRuntime = Effect.fnUntraced(function* (fake: FakeJetski) {
  const adapter = makeJetskiAdapterV2({
    instanceId: JETSKI_INSTANCE_ID,
    settings: jetskiSettings,
    environment: {},
    spawner: fake.spawner,
    fileSystem: yield* FileSystem.FileSystem,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    serverConfig: yield* ServerConfig.ServerConfig,
  });
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: SESSION_ID,
    modelSelection,
    runtimePolicy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  const takeEvent = (predicate: (event: ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        if (predicate(event)) return event;
      }
    });
  return { runtime, takeEvent };
});

const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  runOrdinal: number,
  text: string,
) {
  const now = yield* DateTime.now;
  const appThread = {
    createdBy: "user",
    creationSource: "web",
    id: THREAD_ID,
    projectId: "project:fixture:jetski" as OrchestrationV2AppThread["projectId"],
    title: "Jetski test thread",
    providerInstanceId: JETSKI_INSTANCE_ID,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  } satisfies OrchestrationV2AppThread;
  const runId = RunId.make(`run:${THREAD_ID}:${runOrdinal}`);
  yield* runtime.startTurn({
    appThread,
    threadId: THREAD_ID,
    runId,
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${THREAD_ID}:${runOrdinal}` as never,
      text,
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection,
    runtimePolicy,
  });
});

const isCompletedTerminal = (event: ProviderAdapterV2Event) =>
  event.type === "turn.terminal" && event.status === "completed";

describe("JetskiAdapterV2", () => {
  it.effect("runs a queued message on the same process once the previous turn ends", () =>
    Effect.gen(function* () {
      const fake = yield* makeFakeJetski;
      const { runtime, takeEvent } = yield* openRuntime(fake);
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
      });

      yield* startTurn(runtime, providerThread, 1, "first");
      assert.deepStrictEqual(yield* fake.takeInput, {
        event: "user",
        message: { role: "user", content: "first" },
      });
      yield* fake.emit({ event: "result", result: { status: "SUCCESS" } });
      yield* takeEvent(isCompletedTerminal);

      // The orchestrator starts the queued run right after the terminal event.
      yield* startTurn(runtime, providerThread, 2, "second");
      assert.deepStrictEqual(yield* fake.takeInput, {
        event: "user",
        message: { role: "user", content: "second" },
      });
      yield* fake.emit({ event: "result", result: { status: "SUCCESS" } });
      const terminal = yield* takeEvent(isCompletedTerminal);
      assert.isTrue(terminal.type === "turn.terminal" && terminal.runOrdinal === 2);
      assert.strictEqual(fake.spawnCount(), 1);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});

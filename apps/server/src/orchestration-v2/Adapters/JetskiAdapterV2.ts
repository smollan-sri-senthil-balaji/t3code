/**
 * JetskiAdapterV2 — orchestrator-v2 adapter for Google's jetski-cli, driving
 * its headless stream-json print mode over stdio (see `JetskiCli.ts`).
 *
 * One jetski-cli process serves the session's thread and runs one turn per
 * stdin `user` event; a `result` event terminalizes the turn. The jetski
 * conversation id from `init` is the durable `nativeThreadRef`, so a restarted
 * process resumes with `--conversation <id>`. The process is replaced when the
 * model, runtime mode or cwd changes, and after Stop, because the CLI's own
 * `cancel` event leaves it unresponsive.
 *
 * Headless mode cannot prompt for permissions. Runtime modes map to jetski
 * flags up front, and denied actions are surfaced as a notice on the turn.
 */
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import {
  JetskiSettings,
  ProviderDriverKind,
  type ChatAttachment,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import * as ServerConfig from "../../config.ts";
import { mcpToolPresentation } from "../../provider/McpToolPresentation.ts";
import { mergeProviderInstanceEnvironment } from "../../provider/ProviderInstanceEnvironment.ts";
import * as IdAllocator from "../IdAllocator.ts";
import * as ProviderAdapter from "../ProviderAdapter.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "../ProviderAdapterDriver.ts";
import { makeProviderFailure } from "../ProviderFailure.ts";
import { randomUuidV4 } from "../RandomUuid.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";
import {
  buildJetskiArgs,
  jetskiField as field,
  jetskiModeArgs,
  jetskiString as stringField,
  jetskiNumber as numberField,
  jetskiUserMessage,
  makeJetskiProcess,
  resolveJetskiBinary,
  splitJetskiLaunchArgs,
  type JetskiProcess,
  type JetskiRecord,
} from "./JetskiCli.ts";

export const JETSKI_PROVIDER = ProviderDriverKind.make("jetski");
const DEFAULT_JETSKI_SETTINGS = Schema.decodeSync(JetskiSettings)({});

const STREAM_FLUSH_MS = 50;

const JetskiProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    // Model and mode changes respawn the process with --conversation.
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    supportsRuntimeModeSwitchInSession: true,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: false,
    canRollbackThread: false,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: false,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: false,
    supportsSteeringByInterruptRestart: false,
    // The orchestrator holds queued messages and starts each one as a normal
    // turn after the active turn ends, on the same long-lived process.
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: false,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: false,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: false,
    supportsApplyPatchApproval: false,
    approvalsHaveNativeRequestIds: false,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: false,
    emitsTodoList: false,
    emitsProposedPlan: false,
    supportsStructuredQuestions: false,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: false,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: false,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: false,
    providerRollbackReturnsSnapshot: false,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "weak",
    nativeItemIds: "weak",
    nativeRequestIds: "weak",
  },
} satisfies OrchestrationV2ProviderCapabilities;

/** Tools whose primary argument is the file they modify. */
const JETSKI_FILE_CHANGE_TOOLS = new Set([
  "write_to_file",
  "replace_file_content",
  "multi_replace_file_content",
  "sed_file",
]);

export interface JetskiAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: JetskiSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly fileSystem: FileSystem.FileSystem;
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly serverConfig: ServerConfig.ServerConfig["Service"];
}

function providerRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: JETSKI_PROVIDER, nativeId, strength };
}

type ItemStatus = "running" | "completed" | "failed" | "interrupted";

/** jetski step states are protobuf enum names; only ACTIVE and DONE are observed in practice. */
function stepStatus(state: string | undefined): ItemStatus {
  switch (state) {
    case "DONE":
      return "completed";
    case "ERROR":
    case "FAILED":
      return "failed";
    case "CANCELED":
    case "CANCELLED":
      return "interrupted";
    default:
      return "running";
  }
}

interface StreamItemState {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  readonly startedAt: DateTime.Utc;
}

interface ActiveTurn {
  readonly turnInput: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  readonly streamItems: Map<string, StreamItemState>;
  readonly toolStartedAt: Map<string, DateTime.Utc>;
  readonly toolParams: Map<string, unknown>;
  interrupted: boolean;
}

interface ThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActiveTurn | null;
}

interface LiveProcess {
  readonly process: JetskiProcess;
  readonly scope: Scope.Closeable;
  readonly configKey: string;
}

export function makeJetskiAdapterV2(
  options: JetskiAdapterV2Options,
): ProviderAdapter.ProviderAdapterV2Shape {
  const { idAllocator } = options;

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapter.ProviderAdapterProtocolError({
      driver: JETSKI_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  return ProviderAdapter.ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: JETSKI_PROVIDER,
    getCapabilities: () => Effect.succeed(JetskiProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("JetskiAdapterV2.openSession")(function* (
      input: ProviderAdapter.ProviderAdapterV2OpenSessionInput,
    ) {
      const sessionScope = yield* Effect.scope;
      const sessionCwd = input.runtimePolicy.cwd ?? options.serverConfig.cwd;
      const command = yield* resolveJetskiBinary(
        options.settings.binaryPath,
        options.environment,
      ).pipe(Effect.provideService(FileSystem.FileSystem, options.fileSystem));
      const launchArgs = splitJetskiLaunchArgs(options.settings.launchArgs);

      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: JETSKI_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd: sessionCwd,
        model: input.modelSelection.model,
        capabilities: JetskiProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        ProviderAdapter.ProviderAdapterV2Error | Cause.Done
      >();
      // Process output, turn start and Stop all mutate turn state; one permit
      // keeps `turn.terminal` ordered after the items it closes.
      const permit = yield* Semaphore.make(1);
      let threadState: ThreadState | null = null;
      let live: LiveProcess | null = null;

      const emit = (event: ProviderAdapter.ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession = (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
        patch: Partial<OrchestrationV2ProviderSession> = {},
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, ...patch, status, lastError, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: JETSKI_PROVIDER,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread = (
        state: ThreadState,
        patch: Partial<OrchestrationV2ProviderThread>,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          state.providerThread = { ...state.providerThread, ...patch, updatedAt };
          yield* emit({
            type: "provider_thread.updated",
            driver: JETSKI_PROVIDER,
            providerThread: state.providerThread,
          });
        });

      const itemOrdinal = (turn: ActiveTurn, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const baseItemFields = (
        turn: ActiveTurn,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => ({
        id: idAllocator.derive.turnItemFromProviderItem({ driver: JETSKI_PROVIDER, nativeItemId }),
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        nodeId: idAllocator.derive.nodeFromProviderItem({ driver: JETSKI_PROVIDER, nativeItemId }),
        providerThreadId: turn.turnInput.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: providerRef(nativeItemId, "weak"),
        parentItemId: null,
        ordinal: itemOrdinal(turn, nativeItemId),
        startedAt,
        updatedAt,
      });

      const emitItemNode = (
        turn: ActiveTurn,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) =>
        emit({
          type: "node.updated",
          driver: JETSKI_PROVIDER,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({ driver: JETSKI_PROVIDER, nativeItemId }),
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeItemId, "weak"),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });

      // ── streaming text / reasoning ────────────────────────

      const emitStreamItem = (turn: ActiveTurn, item: StreamItemState, streaming: boolean) =>
        Effect.gen(function* () {
          const emittedAt = yield* DateTime.now;
          const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
          const status = streaming ? "running" : "completed";
          const completedAt = streaming ? null : emittedAt;
          yield* emitItemNode(
            turn,
            item.nativeItemId,
            item.kind,
            status,
            item.startedAt,
            completedAt,
          );
          if (item.kind === "reasoning") {
            yield* emit({
              type: "turn_item.updated",
              driver: JETSKI_PROVIDER,
              turnItem: {
                ...base,
                status,
                title: null,
                completedAt,
                type: "reasoning",
                text: item.text,
                streaming,
              },
            });
            return;
          }
          const messageId = idAllocator.derive.messageFromProviderItem({
            driver: JETSKI_PROVIDER,
            nativeItemId: item.nativeItemId,
          });
          yield* emit({
            type: "turn_item.updated",
            driver: JETSKI_PROVIDER,
            turnItem: {
              ...base,
              status,
              title: null,
              completedAt,
              type: "assistant_message",
              messageId,
              text: item.text,
              streaming,
            },
          });
          yield* emit({
            type: "message.updated",
            driver: JETSKI_PROVIDER,
            message: {
              id: messageId,
              threadId: turn.turnInput.threadId,
              runId: turn.turnInput.runId,
              nodeId: base.nodeId,
              role: "assistant",
              text: item.text,
              attachments: [],
              streaming,
              createdBy: "agent",
              creationSource: "provider",
              createdAt: item.startedAt,
              updatedAt: emittedAt,
            },
          });
        });

      const scheduleStreamFlush = (turn: ActiveTurn, item: StreamItemState) =>
        Effect.suspend(() => {
          if (item.flushScheduled || item.completed) return Effect.void;
          item.flushScheduled = true;
          return Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
            Effect.andThen(
              permit.withPermits(1)(
                Effect.suspend(() => {
                  item.flushScheduled = false;
                  return item.completed ? Effect.void : emitStreamItem(turn, item, true);
                }),
              ),
            ),
            Effect.forkIn(sessionScope),
            Effect.asVoid,
          );
        });

      const streamItemFor = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        kind: StreamItemState["kind"],
        nativeItemId: string,
      ) {
        const existing = turn.streamItems.get(nativeItemId);
        if (existing !== undefined) return existing;
        const item: StreamItemState = {
          nativeItemId,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          startedAt: yield* DateTime.now,
        };
        turn.streamItems.set(nativeItemId, item);
        // Reserve the ordinal on first delta so items keep stream order.
        itemOrdinal(turn, nativeItemId);
        return item;
      });

      const completeStreamItem = (turn: ActiveTurn, item: StreamItemState) =>
        Effect.suspend(() => {
          if (item.completed) return Effect.void;
          item.completed = true;
          return item.text.trim().length === 0 ? Effect.void : emitStreamItem(turn, item, false);
        });

      const appendStreamText = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        kind: StreamItemState["kind"],
        nativeItemId: string,
        delta: string | undefined,
        done: boolean,
      ) {
        if ((delta === undefined || delta.length === 0) && !done) return;
        const item = yield* streamItemFor(turn, kind, nativeItemId);
        if (delta !== undefined) item.text += delta;
        if (done) yield* completeStreamItem(turn, item);
        else yield* scheduleStreamFlush(turn, item);
      });

      // ── tools ─────────────────────────────────────────────

      const emitToolStep = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        nativeItemId: string,
        step: unknown,
      ) {
        const toolInfo = field(step, "tool_info");
        const toolName = stringField(step, "tool_name") ?? stringField(toolInfo, "name") ?? "tool";
        const params = field(toolInfo, "parameters") ?? turn.toolParams.get(nativeItemId) ?? {};
        turn.toolParams.set(nativeItemId, params);
        const output = stringField(toolInfo, "output");
        const status = turn.interrupted ? "interrupted" : stepStatus(stringField(step, "state"));
        const emittedAt = yield* DateTime.now;
        const startedAt = turn.toolStartedAt.get(nativeItemId) ?? emittedAt;
        turn.toolStartedAt.set(nativeItemId, startedAt);
        const completedAt = status === "running" ? null : emittedAt;
        yield* emitItemNode(turn, nativeItemId, "tool_call", status, startedAt, completedAt);
        const shared = {
          ...baseItemFields(turn, nativeItemId, startedAt, emittedAt),
          status,
          title: toolName,
          completedAt,
        } as const;
        if (toolName === "run_command") {
          yield* emit({
            type: "turn_item.updated",
            driver: JETSKI_PROVIDER,
            turnItem: {
              ...shared,
              type: "command_execution",
              input: stringField(params, "CommandLine") ?? "",
              ...(output === undefined ? {} : { output }),
            },
          });
          return;
        }
        const fileName = stringField(params, "TargetFile")?.trim();
        if (JETSKI_FILE_CHANGE_TOOLS.has(toolName) && fileName) {
          const newStr =
            toolName === "write_to_file" ? stringField(params, "CodeContent") : undefined;
          yield* emit({
            type: "turn_item.updated",
            driver: JETSKI_PROVIDER,
            turnItem: {
              ...shared,
              type: "file_change",
              fileName,
              ...(newStr === undefined ? {} : { newStr }),
            },
          });
          return;
        }
        yield* emit({
          type: "turn_item.updated",
          driver: JETSKI_PROVIDER,
          turnItem: {
            ...shared,
            type: "dynamic_tool",
            ...mcpToolPresentation({ toolName }),
            toolName,
            input: params,
            ...(output === undefined ? {} : { output }),
          },
        });
      });

      const emitNotice = Effect.fnUntraced(function* (
        turn: ActiveTurn,
        nativeItemId: string,
        message: string,
      ) {
        const at = yield* DateTime.now;
        yield* emitItemNode(turn, nativeItemId, "system", "completed", at, at);
        yield* emit({
          type: "turn_item.updated",
          driver: JETSKI_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, at, at),
            status: "completed",
            title: null,
            completedAt: at,
            type: "system_notice",
            message,
          },
        });
      });

      // ── turn lifecycle ────────────────────────────────────

      const finalizeTurn = Effect.fnUntraced(function* (
        state: ThreadState,
        failure: ReturnType<typeof makeProviderFailure> | null,
      ) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        const completedAt = yield* DateTime.now;
        yield* Effect.forEach(
          Array.from(turn.streamItems.values()),
          (item) => completeStreamItem(turn, item),
          { discard: true },
        );
        const effectiveFailure = turn.interrupted ? null : failure;
        yield* emit({
          type: "provider_turn.updated",
          driver: JETSKI_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            status: turn.interrupted
              ? "interrupted"
              : effectiveFailure !== null
                ? "failed"
                : "completed",
            completedAt,
          },
        });
        yield* updateProviderThread(state, { status: "idle" });
        yield* updateProviderSession(
          effectiveFailure !== null ? "error" : "ready",
          effectiveFailure?.message ?? null,
        );
        if (effectiveFailure !== null) {
          const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
          yield* emit({
            type: "turn_item.updated",
            driver: JETSKI_PROVIDER,
            turnItem: {
              ...baseItemFields(turn, failureItemId, completedAt, completedAt),
              status: "failed",
              title: null,
              completedAt,
              type: "error",
              failure: effectiveFailure,
            },
          });
          yield* emit({
            type: "turn.terminal",
            driver: JETSKI_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: itemOrdinal(turn, failureItemId),
            status: "failed",
            failure: effectiveFailure,
            threadDisposition: "reusable",
          });
          return;
        }
        yield* emit({
          type: "turn.terminal",
          driver: JETSKI_PROVIDER,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          runOrdinal: turn.turnInput.runOrdinal,
          status: turn.interrupted ? "interrupted" : "completed",
          failure: null,
          threadDisposition: "reusable",
        });
      });

      const rememberConversation = (conversationId: string | undefined) =>
        Effect.suspend(() => {
          const state = threadState;
          if (
            state === null ||
            conversationId === undefined ||
            conversationId.length === 0 ||
            state.providerThread.nativeThreadRef?.nativeId === conversationId
          ) {
            return Effect.void;
          }
          return updateProviderThread(state, { nativeThreadRef: providerRef(conversationId) });
        });

      const handleRecord = Effect.fnUntraced(function* (record: JetskiRecord) {
        const kind = stringField(record, "event");
        if (kind === "init") {
          yield* rememberConversation(stringField(record, "conversation_id"));
          return;
        }
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (kind === "step_update") {
          const step = field(record, "step_update");
          yield* rememberConversation(stringField(step, "conversation_id"));
          if (turn === null) return;
          const stepIndex = numberField(step, "step_index");
          if (stepIndex === undefined) return;
          const nativeItemId = `${turn.providerTurn.id}:s${stepIndex}`;
          const done = stringField(step, "state") === "DONE";
          switch (stringField(step, "step_type")) {
            case "agent_response":
              yield* appendStreamText(
                turn,
                "reasoning",
                `${nativeItemId}:thinking`,
                stringField(step, "thinking_delta"),
                done && turn.streamItems.has(`${nativeItemId}:thinking`),
              );
              yield* appendStreamText(
                turn,
                "assistant_message",
                nativeItemId,
                stringField(step, "text_delta"),
                done,
              );
              return;
            case "tool":
              yield* emitToolStep(turn, nativeItemId, step);
              return;
            default:
              return;
          }
        }
        if (kind === "result") {
          if (state === null || turn === null) return;
          const result = field(record, "result");
          yield* rememberConversation(stringField(result, "conversation_id"));
          const denied = field(result, "denied_actions");
          if (Array.isArray(denied) && denied.length > 0) {
            const names = denied
              .map((entry) => stringField(entry, "display_name") ?? stringField(entry, "action"))
              .filter((name): name is string => name !== undefined);
            yield* emitNotice(
              turn,
              `${turn.providerTurn.id}:denied`,
              `Jetski denied ${names.join(", ") || "a tool call"}: headless mode cannot ask for ` +
                "approval. Switch this thread to a less restrictive access mode, or add an " +
                "allow rule under permissions.allow in jetski's settings.json.",
            );
          }
          const status = stringField(result, "status");
          yield* finalizeTurn(
            state,
            status === "SUCCESS"
              ? null
              : makeProviderFailure({
                  message: stringField(result, "error") || "Jetski turn failed.",
                  class: "provider_error",
                }),
          );
        }
      });

      // ── process management ────────────────────────────────

      /** Stop the live process. Must not run on the process's own pump fiber. */
      const stopLive = Effect.suspend(() => {
        const current = live;
        live = null;
        return current === null
          ? Effect.void
          : current.process.terminate.pipe(
              Effect.andThen(Scope.close(current.scope, Exit.void)),
              Effect.ignore,
            );
      });

      const startLive = Effect.fnUntraced(function* (
        configKey: string,
        args: ReadonlyArray<string>,
        cwd: string,
      ) {
        const processScope = yield* Scope.fork(sessionScope);
        const jetski = yield* makeJetskiProcess({
          command,
          args,
          cwd,
          env: options.environment,
        }).pipe(
          Scope.provide(processScope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, options.spawner),
          Effect.tapError(() => Scope.close(processScope, Exit.void)),
        );
        const entry: LiveProcess = { process: jetski, scope: processScope, configKey };
        live = entry;
        yield* Effect.gen(function* () {
          while (true) {
            const record = yield* Queue.take(jetski.events);
            yield* permit.withPermits(1)(handleRecord(record));
          }
        }).pipe(
          Effect.catchCause((cause) =>
            permit.withPermits(1)(
              Effect.gen(function* () {
                // A deliberate stop clears `live` first; only an unexpected
                // exit fails the running turn. The next turn respawns.
                if (live !== entry) return;
                live = null;
                const state = threadState;
                if (state?.activeTurn != null) {
                  yield* finalizeTurn(
                    state,
                    makeProviderFailure({
                      cause,
                      message: "jetski-cli exited unexpectedly.",
                      class: "transport_error",
                    }),
                  );
                }
                yield* Scope.close(processScope, Exit.void).pipe(Effect.forkIn(sessionScope));
              }),
            ),
          ),
          Effect.forkIn(processScope),
        );
        return jetski;
      });

      /**
       * Return a process matching the model, mode and workspace, restarting
       * the current one (resuming its conversation) when any of them changed.
       */
      const ensureLive = Effect.fnUntraced(function* (
        modelSelection: ProviderAdapter.ProviderAdapterV2TurnInput["modelSelection"],
        runtimePolicy: ProviderAdapter.ProviderAdapterV2TurnInput["runtimePolicy"],
        state: ThreadState,
      ) {
        const cwd = runtimePolicy.cwd ?? sessionCwd;
        const model = modelSelection.model;
        const modeArgs = jetskiModeArgs(runtimePolicy.runtimeMode, runtimePolicy.interactionMode);
        const configKey = [cwd ?? "", model, ...modeArgs].join("\u0000");
        if (live !== null && live.configKey === configKey) return live.process;
        yield* stopLive;
        const args = buildJetskiArgs({
          model,
          conversationId: state.providerThread.nativeThreadRef?.nativeId ?? null,
          modeArgs,
          launchArgs,
        });
        const jetski = yield* startLive(configKey, args, cwd);
        if (model !== sessionEntity.model) {
          yield* updateProviderSession(sessionEntity.status, sessionEntity.lastError, { model });
        }
        return jetski;
      });

      /**
       * jetski-cli takes ~20 s to start. Threads register before the turn's
       * checkpoint is captured, so spawning here overlaps the two; the turn
       * then reuses this process. Failures are left for the turn to surface.
       */
      const prewarm = (threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput) =>
        permit
          .withPermits(1)(
            Effect.suspend(() => {
              const state = threadState;
              return state === null || state.activeTurn !== null
                ? Effect.void
                : ensureLive(threadInput.modelSelection, threadInput.runtimePolicy, state);
            }),
          )
          .pipe(
            Effect.catchCause((cause) =>
              Effect.logDebug("jetski-cli prewarm failed", { cause: Cause.pretty(cause) }),
            ),
            Effect.forkIn(sessionScope),
            Effect.asVoid,
          );

      yield* Scope.addFinalizer(sessionScope, stopLive);

      // ── session runtime ───────────────────────────────────

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapter.ProviderAdapterV2EnsureThreadInput,
      ) {
        if (threadState?.activeTurn != null) {
          return yield* protocolError("Cannot register a Jetski thread while a turn is active");
        }
        const existing = threadInput.existingProviderThread;
        // A process is bound to one conversation; switching threads restarts it.
        if (threadState === null || threadState.providerThread.id !== existing?.id) {
          yield* stopLive;
        }
        const createdAt = yield* DateTime.now;
        const threadKey = yield* randomUuidV4;
        const providerThread: OrchestrationV2ProviderThread =
          existing !== undefined
            ? {
                ...existing,
                providerSessionId: input.providerSessionId,
                status: "idle",
                updatedAt: createdAt,
              }
            : {
                // The jetski conversation id only exists once the process
                // starts, so the row id comes from a T3-side key and the
                // native ref is filled in from `init`.
                id: idAllocator.derive.providerThread({
                  driver: JETSKI_PROVIDER,
                  nativeThreadId: `t3:${threadInput.threadId}:${threadKey}`,
                }),
                driver: JETSKI_PROVIDER,
                providerInstanceId: options.instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadInput.threadId,
                ownerNodeId: null,
                nativeThreadRef: null,
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                createdAt,
                updatedAt: createdAt,
              };
        threadState = { providerThread, activeTurn: null };
        yield* emit({
          type: "provider_thread.updated",
          driver: JETSKI_PROVIDER,
          providerThread,
        });
        yield* prewarm(threadInput);
        return providerThread;
      });

      const resolvePromptText = (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ): string => {
        // jetski reads files itself, so attachments travel as paths.
        const lines = attachments.flatMap((attachment) => {
          const path = resolveAttachmentPath({
            attachmentsDir: options.serverConfig.attachmentsDir,
            attachment,
          });
          return path === null ? [] : [`[Attachment saved at ${path}]`];
        });
        return lines.length === 0 ? text : `${text}\n\n${lines.join("\n")}`;
      };

      const runtime: ProviderAdapter.ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: JETSKI_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterEnsureThreadError({
                  driver: JETSKI_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterResumeThreadError({
                  driver: JETSKI_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (turnInput) =>
          Effect.gen(function* () {
            const state = threadState;
            if (state === null) {
              return yield* protocolError("Jetski session has no registered thread");
            }
            if (state.activeTurn !== null) {
              return yield* protocolError("Jetski thread already has an active turn");
            }
            if (state.providerThread.id !== turnInput.providerThread.id) {
              return yield* protocolError("Jetski turn requested for a different provider thread");
            }
            // Keep the orchestrator's row, but never lose a conversation id
            // learned since it was read.
            state.providerThread = {
              ...turnInput.providerThread,
              nativeThreadRef:
                state.providerThread.nativeThreadRef ?? turnInput.providerThread.nativeThreadRef,
            };
            const text = resolvePromptText(turnInput.message.text, turnInput.message.attachments);
            const startedAt = yield* DateTime.now;
            const syntheticNativeTurnId = `${state.providerThread.id}:attempt:${turnInput.attemptId}`;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({
                driver: JETSKI_PROVIDER,
                nativeTurnId: syntheticNativeTurnId,
              }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: providerRef(syntheticNativeTurnId, "weak"),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const activeTurn: ActiveTurn = {
              turnInput,
              providerTurn,
              itemOrdinals: new Map(),
              nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              streamItems: new Map(),
              toolStartedAt: new Map(),
              toolParams: new Map(),
              interrupted: false,
            };
            yield* permit.withPermits(1)(
              Effect.gen(function* () {
                const jetski = yield* ensureLive(
                  turnInput.modelSelection,
                  turnInput.runtimePolicy,
                  state,
                );
                state.activeTurn = activeTurn;
                yield* jetski.send(jetskiUserMessage(text));
                yield* emit({
                  type: "provider_turn.updated",
                  driver: JETSKI_PROVIDER,
                  threadId: turnInput.threadId,
                  providerTurn,
                });
                yield* updateProviderThread(state, {
                  status: "active",
                  firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
                  lastRunOrdinal: turnInput.runOrdinal,
                });
                yield* updateProviderSession("running", null);
              }).pipe(
                Effect.tapError(() =>
                  Effect.sync(() => {
                    if (state.activeTurn === activeTurn) state.activeTurn = null;
                  }),
                ),
              ),
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver: JETSKI_PROVIDER,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: (steerInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterSteerRunUnsupportedError({
              driver: JETSKI_PROVIDER,
              providerThreadId: steerInput.providerThread.id,
            }),
          ),
        interruptTurn: (interruptInput) =>
          permit
            .withPermits(1)(
              Effect.gen(function* () {
                const state = threadState;
                const turn = state?.activeTurn ?? null;
                if (turn === null) {
                  if (interruptInput.requestRuntimeRestart === true) yield* stopLive;
                  return;
                }
                if (turn.providerTurn.id !== interruptInput.providerTurnId) {
                  return yield* protocolError(
                    `Jetski turn ${interruptInput.providerTurnId} is not active`,
                  );
                }
                turn.interrupted = true;
                // jetski's `cancel` event wedges the process, so Stop kills it
                // and the next turn resumes the conversation in a new one.
                yield* stopLive;
                if (state !== null) yield* finalizeTurn(state, null);
              }),
            )
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterInterruptError({
                    driver: JETSKI_PROVIDER,
                    providerThreadId: interruptInput.providerThread.id,
                    providerTurnId: interruptInput.providerTurnId,
                    cause,
                  }),
              ),
            ),
        respondToRuntimeRequest: (requestInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRuntimeRequestResponseError({
              driver: JETSKI_PROVIDER,
              requestId: requestInput.requestId,
              cause: protocolError("Jetski headless mode has no runtime requests"),
            }),
          ),
        readThreadSnapshot: (snapshotInput) =>
          Effect.succeed({
            providerThread:
              threadState?.providerThread.id === snapshotInput.providerThread.id
                ? threadState.providerThread
                : snapshotInput.providerThread,
            providerTurns: [],
            messages: [],
            runtimeRequests: [],
          }),
        rollbackThread: (rollbackInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterRollbackThreadError({
              driver: JETSKI_PROVIDER,
              providerThreadId: rollbackInput.providerThread.id,
              checkpointId: rollbackInput.target.checkpointId,
              cause: protocolError("Jetski does not support conversation rollback"),
            }),
          ),
        forkThread: (forkInput) =>
          Effect.fail(
            new ProviderAdapter.ProviderAdapterForkThreadError({
              driver: JETSKI_PROVIDER,
              providerThreadId: forkInput.sourceProviderThread.id,
              cause: protocolError("Jetski does not support forking conversations"),
            }),
          ),
      };
      return runtime;
    }),
  });
}

// ── driver ────────────────────────────────────────────────────

export type JetskiAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | ServerConfig.ServerConfig;

export const JetskiAdapterV2Driver: ProviderAdapterDriver<
  JetskiSettings,
  JetskiAdapterV2DriverEnv
> = {
  driverKind: JETSKI_PROVIDER,
  configSchema: JetskiSettings,
  defaultConfig: (): JetskiSettings => DEFAULT_JETSKI_SETTINGS,
  create: Effect.fn("JetskiAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<JetskiSettings>) {
      const hostEnvironment = yield* HostProcessEnvironment;
      return makeJetskiAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        spawner: yield* ChildProcessSpawner.ChildProcessSpawner,
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig,
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: JETSKI_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create Jetski adapter.",
              cause,
            }),
        ),
      ),
  ),
};

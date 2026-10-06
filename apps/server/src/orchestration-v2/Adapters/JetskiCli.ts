/**
 * JetskiCli — stdio NDJSON transport for `jetski-cli` headless print mode.
 *
 * The CLI is spawned as
 * `jetski-cli --input-format stream-json --output-format stream-json -p=`.
 * Each stdin line is one input event (`{"event":"user","message":{...}}`) and
 * runs one turn; stdout carries `init`, `step_update` and per-turn `result`
 * events. The process stays alive between turns until it is terminated.
 *
 * Headless mode cannot prompt for tool permissions (tools that need one are
 * auto-denied and reported in `result.denied_actions`), and the `cancel`
 * input event leaves the process unresponsive, so callers stop a turn by
 * terminating the process and resuming with `--conversation <id>`.
 *
 * Used by `JetskiAdapterV2` for sessions and `JetskiTextGeneration` for
 * one-shot prompts.
 */
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as FileSystem from "effect/FileSystem";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import type { RuntimeMode, ProviderInteractionMode } from "@t3tools/contracts";

import { signalProcessGroup } from "../../process/processGroup.ts";

export class JetskiCliError extends Schema.TaggedError<JetskiCliError>()("JetskiCliError", {
  operation: Schema.String,
  detail: Schema.optional(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message(): string {
    return `jetski-cli ${this.operation} failed${this.detail === undefined ? "" : `: ${this.detail}`}.`;
  }
}

export type JetskiRecord = Record<string, unknown>;

export function jetskiField(input: unknown, key: string): unknown {
  return Predicate.isObject(input) ? input[key] : undefined;
}

export function jetskiString(input: unknown, key: string): string | undefined {
  const value = jetskiField(input, key);
  return Predicate.isString(value) ? value : undefined;
}

export function jetskiNumber(input: unknown, key: string): number | undefined {
  const value = jetskiField(input, key);
  return Predicate.isNumber(value) && Number.isFinite(value) ? value : undefined;
}

/** The settings default. Resolved to the Windows install path when not overridden. */
export const JETSKI_DEFAULT_BINARY = "jetski-cli";

/** Model slug meaning "omit --model": jetski-cli uses the user's configured model. */
export const JETSKI_INHERIT_MODEL_SLUG = "default";

/**
 * Resolve the executable. An explicit path is used as-is. The bare default
 * prefers the standard Windows install location, because the installer does
 * not put jetski-cli on PATH.
 */
export const resolveJetskiBinary = Effect.fn("resolveJetskiBinary")(function* (
  binaryPath: string,
  environment: NodeJS.ProcessEnv,
) {
  const command = binaryPath.trim() || JETSKI_DEFAULT_BINARY;
  if (command !== JETSKI_DEFAULT_BINARY) return command;
  const platform = yield* HostProcessPlatform;
  if (platform !== "win32") return command;
  const programFiles =
    Object.entries(environment).find(([key]) => key.toUpperCase() === "PROGRAMFILES")?.[1] ??
    "C:\\Program Files";
  const installed = `${programFiles}\\Google\\jetski-cli\\jetski-cli.exe`;
  const fileSystem = yield* FileSystem.FileSystem;
  const exists = yield* fileSystem.exists(installed).pipe(Effect.orElseSucceed(() => false));
  return exists ? installed : command;
});

/** Split user launch arguments on whitespace, honoring single and double quotes. */
export function splitJetskiLaunchArgs(input: string): ReadonlyArray<string> {
  const args: string[] = [];
  let current = "";
  let quote: '"' | "'" | null = null;
  let hasToken = false;
  for (const char of input) {
    if (quote !== null) {
      if (char === quote) quote = null;
      else current += char;
      continue;
    }
    if (char === '"' || char === "'") {
      quote = char;
      hasToken = true;
      continue;
    }
    if (/\s/u.test(char)) {
      if (hasToken) args.push(current);
      current = "";
      hasToken = false;
      continue;
    }
    current += char;
    hasToken = true;
  }
  if (hasToken) args.push(current);
  return args;
}

/**
 * Map T3 runtime and interaction modes onto jetski-cli flags. Headless mode
 * cannot ask for approval, so "approval-required" keeps jetski's review
 * preset: reads are allowed and anything needing a prompt is denied.
 */
export function jetskiModeArgs(
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
): ReadonlyArray<string> {
  if (interactionMode === "plan") return ["--mode", "plan"];
  switch (runtimeMode) {
    case "full-access":
      return ["--dangerously-skip-permissions"];
    case "auto":
      return ["--mode", "auto"];
    case "auto-accept-edits":
      return ["--mode", "accept-edits"];
    case "approval-required":
      return [];
  }
}

export interface JetskiLaunchOptions {
  readonly model?: string | undefined;
  readonly conversationId?: string | null | undefined;
  readonly modeArgs?: ReadonlyArray<string>;
  readonly launchArgs?: ReadonlyArray<string>;
}

export function buildJetskiArgs(options: JetskiLaunchOptions): ReadonlyArray<string> {
  const model = options.model?.trim();
  return [
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    ...(options.modeArgs ?? []),
    ...(model && model !== JETSKI_INHERIT_MODEL_SLUG ? ["--model", model] : []),
    ...(options.conversationId ? ["--conversation", options.conversationId] : []),
    ...(options.launchArgs ?? []),
    // Print mode with an empty prompt: turns arrive as stdin events.
    "-p=",
  ];
}

export interface JetskiProcessSpawnOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string | undefined;
  readonly env: NodeJS.ProcessEnv;
}

export interface JetskiProcess {
  /** Write one input event as an NDJSON line. */
  readonly send: (record: JetskiRecord) => Effect.Effect<void, JetskiCliError>;
  /** Output events in arrival order. Fails when the process goes away. */
  readonly events: Queue.Queue<JetskiRecord, JetskiCliError>;
  /** Kill the process tree. Idempotent. */
  readonly terminate: Effect.Effect<void>;
}

const MAX_RECORD_CHARS = 8 * 1024 * 1024;
const TERMINATION_GRACE = Duration.seconds(1);

function makeLineFramer() {
  let buffer = "";
  let dropping = false;
  return (chunk: string): ReadonlyArray<string> => {
    const lines: string[] = [];
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf("\n", start);
      const end = newline < 0 ? chunk.length : newline;
      if (!dropping) {
        if (buffer.length + end - start > MAX_RECORD_CHARS) {
          buffer = "";
          dropping = true;
        } else {
          buffer += chunk.slice(start, end);
        }
      }
      if (newline < 0) break;
      if (!dropping && buffer.length > 0) {
        lines.push(buffer.endsWith("\r") ? buffer.slice(0, -1) : buffer);
      }
      buffer = "";
      dropping = false;
      start = newline + 1;
    }
    return lines;
  };
}

const UnknownFromJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeJsonLine = Schema.decodeSync(UnknownFromJsonString);
const encodeJsonLine = Schema.encodeSync(UnknownFromJsonString);

function parseRecord(line: string): JetskiRecord | undefined {
  try {
    const parsed: unknown = decodeJsonLine(line);
    return Predicate.isObject(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

/** Spawn jetski-cli in the current scope. Closing the scope kills the process tree. */
export const makeJetskiProcess = Effect.fnUntraced(function* (options: JetskiProcessSpawnOptions) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcessPlatform;
  const scope = yield* Effect.scope;

  const spawnCommand = yield* resolveSpawnCommand(options.command, [...options.args], {
    env: options.env,
  }).pipe(Effect.mapError((cause) => new JetskiCliError({ operation: "spawn", cause })));
  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
        env: options.env,
        extendEnv: false,
        shell: spawnCommand.shell,
        detached: platform !== "win32",
      }),
    )
    .pipe(Effect.mapError((cause) => new JetskiCliError({ operation: "spawn", cause })));

  let childExited = false;
  const hasExited = (): boolean => {
    if (childExited) return true;
    try {
      if (platform === "win32") process.kill(Number(child.pid), 0);
      else signalProcessGroup(Number(child.pid), 0);
      return false;
    } catch {
      return true;
    }
  };

  // jetski-cli starts a language server and browser helpers as children, so
  // Windows needs a tree kill and POSIX signals the whole process group.
  const terminateProcess =
    platform === "win32"
      ? Effect.gen(function* () {
          if (hasExited()) return;
          const taskkill = yield* spawner.spawn(
            ChildProcess.make("taskkill", ["/PID", String(child.pid), "/T", "/F"]),
          );
          yield* taskkill.exitCode;
        }).pipe(Effect.scoped, Effect.ignore)
      : Effect.gen(function* () {
          const signal = (name: NodeJS.Signals) => {
            try {
              signalProcessGroup(Number(child.pid), name);
              return true;
            } catch {
              return false;
            }
          };
          if (hasExited() || !signal("SIGTERM")) return;
          yield* Effect.sleep(TERMINATION_GRACE);
          if (!hasExited()) signal("SIGKILL");
        });

  yield* Scope.addFinalizer(scope, terminateProcess.pipe(Effect.ignore, Effect.uninterruptible));

  const events = yield* Queue.unbounded<JetskiRecord, JetskiCliError>();
  const outgoing = yield* Queue.unbounded<Uint8Array, JetskiCliError>();
  const transportDown = yield* Deferred.make<never, JetskiCliError>();

  const failTransport = (error: JetskiCliError) =>
    Effect.gen(function* () {
      const claimed = yield* Deferred.fail(transportDown, error);
      if (!claimed) return;
      yield* Queue.fail(outgoing, error);
      yield* Queue.fail(events, error);
    });

  yield* child.exitCode.pipe(
    Effect.tap(() => Effect.sync(() => (childExited = true))),
    Effect.ignore,
    Effect.forkIn(scope),
  );

  yield* Effect.gen(function* () {
    const frame = makeLineFramer();
    const route = (line: string) => {
      const record = parseRecord(line);
      return record === undefined
        ? Effect.logDebug("Dropping non-JSON jetski-cli stdout line.", { lineLength: line.length })
        : Queue.offer(events, record).pipe(Effect.asVoid);
    };
    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) => Effect.forEach(frame(chunk), route, { discard: true })),
    );
    yield* Effect.forEach(frame("\n"), route, { discard: true });
  }).pipe(
    Effect.matchCauseEffect({
      onFailure: (cause) => failTransport(new JetskiCliError({ operation: "read", cause })),
      onSuccess: () =>
        failTransport(new JetskiCliError({ operation: "read", detail: "process closed stdout" })),
    }),
    Effect.forkIn(scope),
  );

  // stderr carries diagnostics that can include prompt text; log lengths only.
  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      chunk.trim().length === 0
        ? Effect.void
        : Effect.logDebug("jetski-cli stderr", { stderrLength: chunk.length }),
    ),
    Effect.ignore,
    Effect.forkIn(scope),
  );

  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catchCause((cause) => failTransport(new JetskiCliError({ operation: "write", cause }))),
    Effect.forkIn(scope),
  );

  const send = (record: JetskiRecord): Effect.Effect<void, JetskiCliError> =>
    Effect.gen(function* () {
      const accepted = yield* Queue.offer(
        outgoing,
        new TextEncoder().encode(`${encodeJsonLine(record)}\n`),
      );
      if (!accepted) return yield* Deferred.await(transportDown);
    });

  return {
    send,
    events,
    terminate: failTransport(
      new JetskiCliError({ operation: "terminate", detail: "process was stopped" }),
    ).pipe(Effect.andThen(terminateProcess), Effect.ignore, Effect.uninterruptible),
  } satisfies JetskiProcess;
});

/** A user turn as a stream-json input event. */
export function jetskiUserMessage(text: string): JetskiRecord {
  return { event: "user", message: { role: "user", content: text } };
}

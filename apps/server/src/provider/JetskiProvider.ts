/**
 * JetskiProvider — snapshot/probe layer for jetski-cli.
 *
 * `jetski-cli models` doubles as the health check: it needs a working binary
 * and a signed-in account, and prints `slug<TAB>name` per available model.
 * `--stamp` supplies the build label used as the version. `--print /usage`
 * reports quota; it is slow, so the driver reads it off the status path.
 */
import {
  type CustomModelSetting,
  type JetskiSettings,
  type ServerProviderModel,
  type ServerProviderUsageWindow,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import {
  JETSKI_INHERIT_MODEL_SLUG,
  JetskiCliError,
  resolveJetskiBinary,
} from "../orchestration-v2/Adapters/JetskiCli.ts";
import {
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "./providerSnapshot.ts";

const JETSKI_PRESENTATION = {
  displayName: "Jetski",
  // Plan maps to `--mode plan`.
  showInteractionModeToggle: true,
  supportedRuntimeModes: ["approval-required", "auto-accept-edits", "auto", "full-access"],
  reportsContextWindow: false,
  requiresNewThreadForModelChange: false,
} as const;

// `models` fetches from the backend and starts slowly on a cold machine.
const MODELS_PROBE_TIMEOUT_MS = 45_000;
const STAMP_PROBE_TIMEOUT_MS = 10_000;

const EMPTY_JETSKI_MODEL_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });

const JETSKI_DEFAULT_MODEL: ServerProviderModel = {
  slug: JETSKI_INHERIT_MODEL_SLUG,
  name: "Jetski default",
  isCustom: false,
  capabilities: EMPTY_JETSKI_MODEL_CAPABILITIES,
};

function jetskiModels(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  discovered: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    [JETSKI_DEFAULT_MODEL, ...discovered],
    customModels ?? [],
    EMPTY_JETSKI_MODEL_CAPABILITIES,
  );
}

/** Parse `jetski-cli models` output: one `slug<TAB>display name` per line. */
export function parseJetskiModels(output: string): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>([JETSKI_INHERIT_MODEL_SLUG]);
  const models: ServerProviderModel[] = [];
  for (const line of output.split(/\r?\n/u)) {
    const [rawSlug, ...rest] = line.split("\t");
    const slug = rawSlug?.trim();
    if (!slug || rest.length === 0 || /\s/u.test(slug) || seen.has(slug)) continue;
    seen.add(slug);
    models.push({
      slug,
      name: rest.join("\t").trim() || slug,
      isCustom: false,
      capabilities: EMPTY_JETSKI_MODEL_CAPABILITIES,
    });
  }
  return models;
}

/** Extract the dated build from `Build label: jetski-cli.gwindows_20261002.00_p0`. */
export function parseJetskiVersion(output: string): string | null {
  const label = /Build label:\s*(\S+)/u.exec(output)?.[1];
  if (label === undefined) return null;
  return /_(\d{8}\.\d+)/u.exec(label)?.[1] ?? label;
}

const runJetski = (command: string, args: ReadonlyArray<string>, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(command, [...args], { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

// jetski-cli answers `/usage` itself; it takes about 40 s even when warm.
const USAGE_PROBE_TIMEOUT_MS = 90_000;

/** One row of `jetski-cli --print /usage`. */
export interface JetskiUsageRow {
  readonly label: string;
  readonly window: string;
  readonly remainingPercent: number;
  readonly resetsAt?: string;
}

/** Parse `/usage` output: `Label<TAB>Window<TAB>NN%<TAB>reset ISO` per line. */
export function parseJetskiUsage(output: string): ReadonlyArray<JetskiUsageRow> {
  const rows: JetskiUsageRow[] = [];
  for (const line of output.split(/\r?\n/u)) {
    const [label, window, remaining, resetsAt] = line.split("\t").map((part) => part.trim());
    const percent =
      remaining === undefined ? undefined : /^(\d+(?:\.\d+)?)%$/u.exec(remaining)?.[1];
    if (!label || !window || percent === undefined) continue;
    rows.push({
      label,
      window,
      remainingPercent: Math.min(100, Number(percent)),
      ...(resetsAt && !Number.isNaN(Date.parse(resetsAt)) ? { resetsAt } : {}),
    });
  }
  return rows;
}

const HOUR_WORDS: Record<string, number> = {
  one: 1,
  two: 2,
  three: 3,
  four: 4,
  five: 5,
  six: 6,
  eight: 8,
  twelve: 12,
  twenty: 20,
};

/** "Twenty Hour Limit Remaining" → 1200. Unknown phrasing has no duration. */
function jetskiWindowMinutes(window: string): number | undefined {
  if (/\bweek/iu.test(window)) return 7 * 24 * 60;
  const word = /\b(\w+)[ -]hour/iu.exec(window)?.[1]?.toLowerCase();
  if (word === undefined) return undefined;
  const hours = /^\d+$/u.test(word) ? Number(word) : HOUR_WORDS[word];
  return hours === undefined ? undefined : hours * 60;
}

export function jetskiUsageWindows(
  rows: ReadonlyArray<JetskiUsageRow>,
): ReadonlyArray<ServerProviderUsageWindow> {
  return rows.map((row): ServerProviderUsageWindow => {
    const minutes = jetskiWindowMinutes(row.window);
    return {
      id:
        row.label
          .toLowerCase()
          .replace(/[^a-z0-9]+/gu, "-")
          .replace(/^-|-$/gu, "") || "quota",
      kind: minutes === 7 * 24 * 60 ? "weekly" : "other",
      label: row.label,
      usedPercent: 100 - row.remainingPercent,
      ...(row.resetsAt === undefined ? {} : { resetsAt: row.resetsAt }),
      ...(minutes === undefined ? {} : { windowDurationMins: minutes }),
    };
  });
}

/** Read quota with a one-shot process; the stream-json session refuses `/usage`. */
export const readJetskiUsage = (command: string, environment: NodeJS.ProcessEnv) =>
  runJetski(command, ["--print", "/usage"], environment).pipe(
    Effect.timeoutOption(USAGE_PROBE_TIMEOUT_MS),
    Effect.mapError((cause) => new JetskiCliError({ operation: "usage", cause })),
    Effect.flatMap((result) => {
      if (Option.isNone(result)) {
        return Effect.fail(new JetskiCliError({ operation: "usage", detail: "timed out" }));
      }
      const rows = parseJetskiUsage(result.value.stdout);
      if (rows.length > 0) return Effect.succeed(rows);
      const output = (result.value.stderr.trim() || result.value.stdout.trim()).slice(0, 500);
      return Effect.fail(
        new JetskiCliError({
          operation: "usage",
          detail: output || `exited with code ${result.value.code}`,
        }),
      );
    }),
  );

function disabledSnapshot(settings: JetskiSettings, checkedAt: string): ServerProviderDraft {
  return buildServerProvider({
    presentation: JETSKI_PRESENTATION,
    enabled: false,
    checkedAt,
    models: jetskiModels(settings.customModels),
    probe: {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "Jetski is disabled in T3 Code settings.",
    },
  });
}

export function buildInitialJetskiProviderSnapshot(
  settings: JetskiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    if (!settings.enabled) return disabledSnapshot(settings, checkedAt);
    return buildServerProvider({
      presentation: JETSKI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: jetskiModels(settings.customModels),
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking jetski-cli availability...",
      },
    });
  });
}

export const checkJetskiProviderStatus = Effect.fn("checkJetskiProviderStatus")(function* (
  settings: JetskiSettings,
  environment: NodeJS.ProcessEnv,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  if (!settings.enabled) return disabledSnapshot(settings, checkedAt);
  const fallbackModels = jetskiModels(settings.customModels);
  const command = yield* resolveJetskiBinary(settings.binaryPath, environment);

  const errorSnapshot = (installed: boolean, message: string, version: string | null = null) =>
    buildServerProvider({
      presentation: JETSKI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: { installed, version, status: "error", auth: { status: "unknown" }, message },
    });

  const [modelsResult, stampResult] = yield* Effect.all(
    [
      runJetski(command, ["models"], environment).pipe(
        Effect.timeoutOption(MODELS_PROBE_TIMEOUT_MS),
        Effect.result,
      ),
      runJetski(command, ["--stamp"], environment).pipe(
        Effect.timeoutOption(STAMP_PROBE_TIMEOUT_MS),
        Effect.option,
      ),
    ],
    { concurrency: "unbounded" },
  );

  const version = Option.flatten(stampResult).pipe(
    Option.map((stamp) => parseJetskiVersion(`${stamp.stdout}\n${stamp.stderr}`)),
    Option.getOrNull,
  );

  if (Result.isFailure(modelsResult)) {
    const error = modelsResult.failure;
    yield* Effect.logWarning("jetski-cli health check failed.", { errorTag: error._tag });
    return isCommandMissingCause(error)
      ? errorSnapshot(
          false,
          "jetski-cli was not found. Install it or set the binary path in Jetski settings.",
        )
      : errorSnapshot(true, "Failed to run jetski-cli.");
  }
  if (Option.isNone(modelsResult.success)) {
    return errorSnapshot(true, "jetski-cli timed out while listing models.", version);
  }
  const output = modelsResult.success.value;
  const discovered = parseJetskiModels(output.stdout);
  if (output.code !== 0 || discovered.length === 0) {
    return buildServerProvider({
      presentation: JETSKI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "warning",
        auth: { status: "unauthenticated", type: "jetski" },
        message: "jetski-cli listed no models. Run `jetski-cli` in a terminal and sign in.",
      },
    });
  }
  return buildServerProvider({
    presentation: JETSKI_PRESENTATION,
    enabled: true,
    checkedAt,
    models: jetskiModels(settings.customModels, discovered),
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "authenticated", type: "jetski" },
    },
  });
});

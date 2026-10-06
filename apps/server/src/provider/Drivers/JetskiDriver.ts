/**
 * JetskiDriver — `ProviderDriver` for Google's jetski-cli, composing the
 * orchestrator-v2 adapter (`JetskiAdapterV2`), the snapshot/probe layer
 * (`JetskiProvider`), and Jetski-backed text generation.
 *
 * Jetski keeps conversations, auth and settings in the user's own
 * `~/.gemini/jetski`, so continuation identity uses the default grouping.
 */
import {
  JetskiSettings,
  ProviderDriverKind,
  type ServerProvider,
  type ServerProviderUsageLimits,
} from "@t3tools/contracts";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerSettings from "../../serverSettings.ts";
import {
  JetskiAdapterV2Driver,
  type JetskiAdapterV2DriverEnv,
} from "../../orchestration-v2/Adapters/JetskiAdapterV2.ts";
import { resolveJetskiBinary } from "../../orchestration-v2/Adapters/JetskiCli.ts";
import { makeJetskiTextGeneration } from "../../textGeneration/JetskiTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import {
  buildInitialJetskiProviderSnapshot,
  checkJetskiProviderStatus,
  jetskiUsageWindows,
  readJetskiUsage,
  type JetskiUsageRow,
} from "../JetskiProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { makeUsageLimits } from "../providerUsageLimits.ts";
import type { ServerProviderShape } from "../ServerProvider.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeJetskiSettings = Schema.decodeSync(JetskiSettings);

const DRIVER_KIND = ProviderDriverKind.make("jetski");
// jetski-cli updates itself through its own background updater.
const MAINTENANCE_CAPABILITIES = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

export type JetskiDriverEnv =
  | JetskiAdapterV2DriverEnv
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | ServerSettings.ServerSettingsService;

export const JetskiDriver: ProviderDriver<JetskiSettings, JetskiDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Jetski",
    supportsMultipleInstances: true,
  },
  configSchema: JetskiSettings,
  defaultConfig: (): JetskiSettings => decodeJetskiSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const serverSettings = yield* ServerSettings.ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(
        environment,
        yield* HostProcessEnvironment,
      );
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = (snapshot: ServerProviderDraft): ServerProvider => ({
        ...snapshot,
        instanceId,
        driver: DRIVER_KIND,
        ...(displayName ? { displayName } : {}),
        ...(accentColor ? { accentColor } : {}),
        continuation: { groupKey: continuationIdentity.continuationKey },
      });
      const effectiveConfig = { ...config, enabled } satisfies JetskiSettings;

      // `--print /usage` takes ~40 s, three times the status probe, so it runs
      // as enrichment after a ready probe. The last read is re-attached to each
      // probe, which would otherwise publish no limits and clear the bars.
      // Enrichment can't publish limits itself, hence `applyUsageLimits`.
      const lastUsage = yield* Ref.make<ServerProviderUsageLimits | undefined>(undefined);
      const snapshotReady = yield* Deferred.make<ServerProviderShape>();
      const recordUsage = (rows: ReadonlyArray<JetskiUsageRow>) =>
        Effect.gen(function* () {
          const checkedAt = DateTime.formatIso(yield* DateTime.now);
          const windows = jetskiUsageWindows(rows);
          yield* Ref.set(lastUsage, makeUsageLimits({ checkedAt, windows }));
          const snapshot = yield* Deferred.await(snapshotReady);
          yield* snapshot.applyUsageLimits({ checkedAt, windows });
        });

      const checkProvider = checkJetskiProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.flatMap((draft) =>
          draft.status !== "ready"
            ? Effect.succeed(draft)
            : Ref.get(lastUsage).pipe(
                Effect.map((usageLimits) => (usageLimits ? { ...draft, usageLimits } : draft)),
              ),
        ),
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<JetskiSettings>>({
        resolveMaintenance: () => Effect.succeed(MAINTENANCE_CAPABILITIES),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialJetskiProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: ({ snapshot: probed }) =>
          probed.status !== "ready"
            ? Effect.void
            : resolveJetskiBinary(effectiveConfig.binaryPath, processEnv).pipe(
                Effect.flatMap((command) => readJetskiUsage(command, processEnv)),
                Effect.flatMap(recordUsage),
                Effect.catch((error) =>
                  Effect.logDebug("jetski-cli usage read failed.", { detail: error.message }),
                ),
                Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
                Effect.provideService(FileSystem.FileSystem, fileSystem),
              ),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build Jetski snapshot.",
              cause,
            }),
        ),
      );
      yield* Deferred.succeed(snapshotReady, snapshot);

      const orchestrationAdapter = yield* JetskiAdapterV2Driver.create({
        instanceId,
        displayName,
        accentColor,
        environment,
        enabled,
        config,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build Jetski orchestration adapter.",
              cause,
            }),
        ),
      );
      const textGeneration = yield* makeJetskiTextGeneration(effectiveConfig, processEnv);

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        orchestrationAdapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};

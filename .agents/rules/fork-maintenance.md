---
trigger: always_on
---

# Maintaining this fork

This checkout is a private fork of T3 Code (`pingdotgg/t3code`), pushed to
`git@github.com:smollan-sri-senthil-balaji/t3code.git` as `origin`. It adds one
thing upstream can't take: a **Jetski** provider (driver kind `jetski`) that
runs Google's internal `jetski-cli`. The maintainer runs the built desktop app
natively on Windows.

Read this before changing, rebasing, building, or committing anything here.
`AGENTS.md` still applies; this file adds the fork's own rules on top.

## One commit on top of upstream

All fork changes live in **one commit** on top of upstream `main`, titled
`feat(server): add jetski-cli provider`. That keeps upstream syncs to a single
rebase with one set of conflicts.

- Fold every new fork change into that commit (`git commit --amend`, or
  `git reset --soft <upstream base>` and recommit). Never leave a second fork
  commit behind.
- Rewriting the commit means `origin` needs a force push. The maintainer pushes
  themselves; never run `git push`.
- Commit only when the maintainer asks. Keep the commit message current: it
  should describe the whole fork, not the latest edit.
- Never open PRs, upstream or on the fork.

To sync with upstream, run `just sync` (`just init` adds the `upstream`
remote). It runs:

```sh
git fetch upstream
git rebase upstream/main
```

Conflicts land only in the shared files listed below. Resolve them, then run
the checks in [Verifying](#verifying) and rebuild.

## Keep the footprint small

New behavior goes in new `Jetski*` files. Shared upstream files get the
smallest possible hook. These are the only upstream files the fork touches;
adding to this list needs a good reason:

- `packages/contracts/src/settings.ts`: `JetskiSettings` and its registration.
- `packages/contracts/src/model.ts`: jetski entries in the per-driver maps.
- `apps/server/src/provider/builtInDrivers.ts`: registers `JetskiDriver`.
- `apps/server/src/provider/model-manifest.json`: jetski compatibility entry.
- `apps/server/src/provider/ProviderRegistry.test.ts`: `"jetski"` in
  the expected driver list.
- `apps/web/src/components/settings/providerDriverMeta.ts`: settings metadata.

Fork-only files: `orchestration-v2/Adapters/JetskiCli.ts`,
`orchestration-v2/Adapters/JetskiAdapterV2.ts`,
`provider/Drivers/JetskiDriver.ts`, `provider/JetskiProvider.ts`,
`textGeneration/JetskiTextGeneration.ts` (all under `apps/server/src`), their
tests, `docs/user/providers-jetski.md`, `justfile`,
`scripts/fork-build-win.ps1`, and this file.

Prefer the smallest change that works. Reuse upstream machinery (snapshot
enrichment, `applyUsageLimits`, `/usage-limits`) instead of building
Jetski-specific UI or commands. Mobile needs no changes beyond typechecking.

## Decisions already made

- **Jetski is its own provider**, not an ACP agent or a variant of Antigravity.
- **One long-lived jetski-cli per session** in `--input-format stream-json
--output-format stream-json -p=` mode. Each stdin `user` event runs one turn.
  The conversation id from `init` is the thread's `nativeThreadRef`, and a
  restarted process resumes with `--conversation <id>`.
- **Stop kills the process.** jetski-cli's `cancel` event wedges it.
- **The process starts when the thread registers** (`prewarm`), not on the
  first message, because cold start is 18–22 s.
- **Headless mode can't ask for approval.** Runtime modes map to jetski flags
  up front, and denied actions show as a notice on the turn.
- **No `/usage` command.** jetski-cli refuses `/usage` in stream-json mode. A
  local `/usage` turn was built and then removed: `/usage-limits` and
  Usage → Limits show the same numbers without waiting. Quota comes from a
  one-shot `jetski-cli --print /usage` that runs as snapshot enrichment after a
  ready status probe, because it takes ~40 s.

## jetski-cli facts

- Windows install: `C:\Program Files\Google\jetski-cli\jetski-cli.exe`, not on
  `PATH`. `resolveJetskiBinary` finds it.
- Timings on the maintainer's machine: `models` ~14 s, `--print /usage`
  ~40 s, cold start to `init` 18–22 s, warm turn ~5 s.
- `--print /usage` prints `Label<TAB>Window<TAB>NN%<TAB>reset ISO` per line.
  `--output-format json` with `/usage` prints nothing and exits 1.
- **Never kill jetski-cli by name.** The maintainer runs their own jetski-cli
  sessions. Kill only PIDs you spawned (`taskkill /PID <pid> /T /F`).

## Building the Windows app

The maintainer runs a portable copy pinned to Start, so rebuild after any
change they want to try. From the repo root (about 6 minutes):

```powershell
just init    # once per machine: setup, then a build
just build   # every rebuild after that
```

Both run `scripts/fork-build-win.ps1`. It builds the app, mirrors the unpacked
app to `%LOCALAPPDATA%\T3 Code Fork`, points the "T3 Code Fork" Start menu
shortcut at it, and leaves the NSIS installer in `release\`. The copy step
fails if the app is running from that folder; ask the maintainer to close it
rather than killing it. `-Init` skips any setup step that is already done.
The script sets up what the build needs:

- The machine npm config (`C:\ProgramData\npm\npmrc`) points at a corporate
  proxy that lacks some packages. The fork overrides it **for this repo only**:
  a repo-local `.npmrc` with `registry=https://registry.npmjs.org/`, listed in
  `.git/info/exclude` and never committed, plus `pnpm_config_registry` for the
  build. pnpm 11 ignores `npm_config_registry`. Don't touch the machine config.
- The installed MSVC lacks Spectre libraries, so the resource monitor is
  prebuilt with cargo and reused via `T3CODE_DESKTOP_REUSE_RESOURCE_MONITOR`.
- `T3CODE_DESKTOP_KEEP_STAGE` keeps the staged `win-unpacked` folder that the
  script copies.
- The build's final self-check can fail with `EBUSY` on a `.node` file after
  the app is built. That's harmless; the script checks the log's `Done` line
  instead of the exit code and cleans up `%TEMP%\t3code-bundle-selfcheck-*`.

## Verifying

Run targeted checks only, never repo-wide ones:

```powershell
Push-Location apps/server; ..\..\node_modules\.bin\tsc.cmd --noEmit; Pop-Location
corepack pnpm exec vp lint <files>
corepack pnpm exec vp fmt <files>
corepack pnpm exec vp test run apps/server/src/provider/JetskiProvider.test.ts apps/server/src/orchestration-v2/Adapters/JetskiCli.test.ts apps/server/src/provider/ProviderRegistry.test.ts
```

Ignore `: suggestion` lines in tsc output. Typecheck `packages/contracts` too
if you touch it.

The installed app uses `~/.t3/userdata`. Read it only with shared read access;
never start a dev server against it.

## Working in this environment

- The pwsh tool runs in constrained language mode: no inline .NET calls such as
  `[System.Diagnostics.Stopwatch]`. Use cmdlets (`Measure-Command`) or put the
  code in a `.ps1` and run it with `pwsh -NoProfile -File`.
- Use `git grep` for searches.
- The pre-commit hook runs fmt. Write commit messages to a file and use
  `git commit -F <file>`.
- A WSL checkout at `\\wsl.localhost\NixOS\home\symph\Projects\web\t3code` may
  be out of date. The Windows checkout is the one that builds.
- Write in plain, concrete language. Keep replies short.

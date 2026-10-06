# Builds the Windows app from this checkout and refreshes a portable copy at a
# fixed path, so a pinned shortcut always starts the latest build. Run through
# the justfile: `just init` once on a new machine, then `just build`.
#
#   pwsh -NoProfile -File scripts/fork-build-win.ps1 [-Init] [-Dest <dir>]
#
# -Init first sets up the machine: the repo-only npm registry, the upstream
# remote, Python for native modules, dependencies, and the prebuilt resource
# monitor. Every step is skipped when it is already done.
#
# The app lands in <Dest> and gets a "T3 Code Fork" Start menu shortcut. Close
# the app before the copy step; Windows locks the files of a running app. The
# NSIS installer is also written to release\.
param(
  [switch]$Init,
  [string]$Dest = (Join-Path $env:LOCALAPPDATA "T3 Code Fork")
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$log = Join-Path $env:TEMP "t3-build.log"
$registry = "https://registry.npmjs.org/"
$python = Join-Path $env:APPDATA "uv\python\cpython-3.14-windows-x86_64-none\python.exe"
$monitor = Join-Path $repo "native\resource-monitor\target\x86_64-pc-windows-msvc\release\t3-resource-monitor.exe"

$env:PYTHON = $python
# pnpm 11 reads pnpm_config_registry, not npm_config_registry.
$env:pnpm_config_registry = $registry
$env:T3CODE_DESKTOP_REUSE_RESOURCE_MONITOR = "true"
# Keeps the staged win-unpacked folder, which is the portable app.
$env:T3CODE_DESKTOP_KEEP_STAGE = "true"

Push-Location $repo
try {
  if ($Init) {
    # The machine npm config points at a proxy that lacks some packages.
    # Override it for this repo only, and keep the override out of git.
    if (-not (Select-String -Path .npmrc -Pattern "^registry=" -Quiet -ErrorAction SilentlyContinue)) {
      Add-Content -Path .npmrc -Value "registry=$registry"
    }
    $exclude = git rev-parse --git-path info/exclude
    if (-not (Select-String -Path $exclude -Pattern "^/?\.npmrc$" -Quiet -ErrorAction SilentlyContinue)) {
      Add-Content -Path $exclude -Value ".npmrc"
    }

    if (-not (git remote | Select-String -Pattern "^upstream$" -Quiet)) {
      git remote add upstream https://github.com/pingdotgg/t3code.git
    }

    if (-not (Test-Path $python)) {
      Write-Output "Installing Python 3.14 with uv..."
      uv python install 3.14
      if (-not (Test-Path $python)) { throw "Python not found at $python" }
    }

    Write-Output "Installing dependencies..."
    corepack pnpm install
    if ($LASTEXITCODE -ne 0) { throw "pnpm install failed." }

    # The build's own MSVC check wants Spectre libraries this machine lacks, so
    # the resource monitor is built once here and reused.
    if (-not (Test-Path $monitor)) {
      Write-Output "Building the resource monitor..."
      cargo build --locked --release --manifest-path native/resource-monitor/Cargo.toml --target x86_64-pc-windows-msvc
      if ($LASTEXITCODE -ne 0 -or -not (Test-Path $monitor)) { throw "Resource monitor build failed." }
    }
  }

  $started = Get-Date
  Write-Output "Building (about 6 minutes, log: $log)..."
  corepack pnpm run dist:desktop:win:x64 *> $log
  $buildExit = $LASTEXITCODE
} finally {
  Pop-Location
}

# The final self-check can fail with EBUSY after the app is built, so trust the
# "Done" line over the exit code.
$done = Select-String -Path $log -Pattern "\[desktop-artifact\] Done\." -Quiet
$stage = Get-ChildItem $env:TEMP -Directory -Filter "t3code-desktop-win-stage-*" |
  Where-Object CreationTime -ge $started |
  Sort-Object CreationTime -Descending |
  Select-Object -First 1
if (-not $done -or $null -eq $stage) {
  throw "Build failed (exit $buildExit). See $log"
}
$unpacked = Join-Path $stage.FullName "app\dist\win-unpacked"
if (-not (Test-Path $unpacked)) { throw "No win-unpacked folder in $($stage.FullName)" }

$running = Get-Process | Where-Object { $_.Path -like "$Dest\*" }
if ($running) { throw "T3 Code is running from $Dest. Close it and run this again." }

robocopy $unpacked $Dest /MIR /NFL /NDL /NJH /NJS /NP | Out-Null
if ($LASTEXITCODE -ge 8) { throw "Copying to $Dest failed (robocopy exit $LASTEXITCODE)." }

Remove-Item $stage.FullName -Recurse -Force
Get-ChildItem $env:TEMP -Directory -Filter "t3code-bundle-selfcheck-*" |
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue

# Point the Start menu shortcut at the app; pin it from Start once.
$exe = Get-ChildItem $Dest -Filter "T3 Code*.exe" | Select-Object -First 1
if ($null -eq $exe) { throw "No T3 Code executable in $Dest" }
$link = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\T3 Code Fork.lnk"
$shortcut = (New-Object -ComObject WScript.Shell).CreateShortcut($link)
$shortcut.TargetPath = $exe.FullName
$shortcut.WorkingDirectory = $Dest
$shortcut.IconLocation = "$($exe.FullName),0"
$shortcut.Description = "T3 Code with the Jetski provider (fork build)"
$shortcut.Save()

Write-Output "Updated $Dest"
exit 0

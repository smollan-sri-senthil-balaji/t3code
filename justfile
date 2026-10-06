# Fork-only helpers for building and maintaining this fork on Windows.
# See .agents/rules/fork-maintenance.md.

set windows-shell := ["pwsh", "-NoProfile", "-Command"]

# Set up this machine once, build the app, and add the Start menu shortcut.
init:
    pwsh -NoProfile -File scripts/fork-build-win.ps1 -Init

# Rebuild the app and refresh the pinned portable copy.
build:
    pwsh -NoProfile -File scripts/fork-build-win.ps1

# Rebase the fork commit onto the latest upstream main.
sync:
    git fetch upstream
    git rebase upstream/main

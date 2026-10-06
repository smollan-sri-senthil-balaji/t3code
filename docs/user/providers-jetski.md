# Jetski

T3 Code can run threads through `jetski-cli`, using the models and account you already have
signed in to Jetski.

## Set Up Jetski

1. Install jetski-cli on the machine running the T3 Code server and sign in by running
   `jetski-cli` once in a terminal.
2. Open T3 Code Settings, enable Jetski, and refresh the provider.

On Windows, T3 Code finds jetski-cli in `C:\Program Files\Google\jetski-cli` even though the
installer does not add it to `PATH`. Anywhere else, or for a different build, set Jetski's binary
path to the executable. Launch arguments are passed to every jetski-cli session.

The model picker lists the models from `jetski-cli models`. **Jetski default** uses whatever model
jetski-cli is configured to use.

## Permission Modes

jetski-cli runs without a terminal, so it can't ask for approval. Each composer mode maps to a
jetski-cli mode:

- **Supervised** uses jetski-cli's review mode. Reads still work. Commands and edits that need
  approval are denied, and the denied actions show up in the work log.
- **Auto-accept edits** allows file edits.
- **Auto** uses jetski-cli's auto mode.
- **Full access** skips permission checks.
- **Plan** runs jetski-cli in plan mode.

Changing the model, mode, or workspace restarts jetski-cli and resumes the same conversation.

## Quota

**Usage → Limits** and `/usage-limits` show how much of each Jetski quota is left and when it
resets. The numbers refresh with the provider status; refresh Jetski in Settings for a fresh read,
which takes jetski-cli about 40 seconds.

## Limitations

- Stopping a turn restarts jetski-cli, so the next message takes a few seconds longer to start.
- Rollback, forks, the context meter, and structured questions are not available.

## Troubleshooting

- If Jetski is unavailable, confirm that the configured binary runs on the server machine, then
  refresh the provider in Settings.
- If only **Jetski default** appears, run `jetski-cli models` in a terminal and check that you are
  signed in.

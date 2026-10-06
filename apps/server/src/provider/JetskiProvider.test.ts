import * as NodeAssert from "node:assert/strict";

import { describe, it } from "vite-plus/test";

import {
  jetskiUsageWindows,
  parseJetskiModels,
  parseJetskiUsage,
  parseJetskiVersion,
} from "./JetskiProvider.ts";

describe("parseJetskiModels", () => {
  it("reads slug and name pairs and skips noise, duplicates and the reserved default", () => {
    const models = parseJetskiModels(
      [
        "Fetching models...",
        "gemini-pro\tGemini Pro",
        "gemini-flash\tGemini Flash\r",
        "gemini-pro\tDuplicate",
        "default\tShould be skipped",
        "bad slug\tName",
        "",
      ].join("\n"),
    );
    NodeAssert.deepEqual(
      models.map((model) => [model.slug, model.name]),
      [
        ["gemini-pro", "Gemini Pro"],
        ["gemini-flash", "Gemini Flash"],
      ],
    );
  });
});

describe("parseJetskiVersion", () => {
  it("extracts the dated build from the build label", () => {
    NodeAssert.equal(
      parseJetskiVersion("Build label: jetski-cli.gwindows_20261002.00_p0\nBuild time: x"),
      "20261002.00",
    );
  });

  it("falls back to the raw label or null", () => {
    NodeAssert.equal(parseJetskiVersion("Build label: dev-build"), "dev-build");
    NodeAssert.equal(parseJetskiVersion("no label here"), null);
  });
});

const USAGE_OUTPUT = [
  "Gemini Pro Next\tTwenty Hour Limit Remaining\t100%\t2026-10-07T07:34:04Z",
  "Gemini Flash Lite\tTwenty Hour Limit Remaining\t99%\t2026-10-07T01:18:42Z\r",
  "Third-Party Models\tWeekly Limit Remaining\t76%\tnot-a-date",
  "Loading quota...",
  "Broken\tTwenty Hour Limit Remaining\tlots\t2026-10-07T01:18:42Z",
  "",
].join("\n");

describe("parseJetskiUsage", () => {
  it("reads quota rows and skips noise and malformed lines", () => {
    NodeAssert.deepEqual(parseJetskiUsage(USAGE_OUTPUT), [
      {
        label: "Gemini Pro Next",
        window: "Twenty Hour Limit Remaining",
        remainingPercent: 100,
        resetsAt: "2026-10-07T07:34:04Z",
      },
      {
        label: "Gemini Flash Lite",
        window: "Twenty Hour Limit Remaining",
        remainingPercent: 99,
        resetsAt: "2026-10-07T01:18:42Z",
      },
      { label: "Third-Party Models", window: "Weekly Limit Remaining", remainingPercent: 76 },
    ]);
  });
});

describe("jetskiUsageWindows", () => {
  it("maps rows to usage windows with durations from the window name", () => {
    NodeAssert.deepEqual(jetskiUsageWindows(parseJetskiUsage(USAGE_OUTPUT)), [
      {
        id: "gemini-pro-next",
        kind: "other",
        label: "Gemini Pro Next",
        usedPercent: 0,
        resetsAt: "2026-10-07T07:34:04Z",
        windowDurationMins: 1200,
      },
      {
        id: "gemini-flash-lite",
        kind: "other",
        label: "Gemini Flash Lite",
        usedPercent: 1,
        resetsAt: "2026-10-07T01:18:42Z",
        windowDurationMins: 1200,
      },
      {
        id: "third-party-models",
        kind: "weekly",
        label: "Third-Party Models",
        usedPercent: 24,
        windowDurationMins: 10080,
      },
    ]);
  });
});

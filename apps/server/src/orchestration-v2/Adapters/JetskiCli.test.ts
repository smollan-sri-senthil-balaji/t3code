import * as NodeAssert from "node:assert/strict";

import { describe, it } from "vite-plus/test";

import { buildJetskiArgs, jetskiModeArgs, splitJetskiLaunchArgs } from "./JetskiCli.ts";

describe("splitJetskiLaunchArgs", () => {
  it("splits on whitespace and keeps quoted values together", () => {
    NodeAssert.deepEqual(splitJetskiLaunchArgs(` --a  "two words" 'x y'  --b=""  `), [
      "--a",
      "two words",
      "x y",
      "--b=",
    ]);
  });

  it("returns nothing for blank input", () => {
    NodeAssert.deepEqual(splitJetskiLaunchArgs("   "), []);
  });
});

describe("jetskiModeArgs", () => {
  it("lets plan mode win over the runtime mode", () => {
    NodeAssert.deepEqual(jetskiModeArgs("full-access", "plan"), ["--mode", "plan"]);
  });

  it("maps runtime modes onto jetski flags", () => {
    NodeAssert.deepEqual(jetskiModeArgs("full-access", "default"), [
      "--dangerously-skip-permissions",
    ]);
    NodeAssert.deepEqual(jetskiModeArgs("auto", "default"), ["--mode", "auto"]);
    NodeAssert.deepEqual(jetskiModeArgs("auto-accept-edits", "default"), [
      "--mode",
      "accept-edits",
    ]);
    NodeAssert.deepEqual(jetskiModeArgs("approval-required", "default"), []);
  });
});

describe("buildJetskiArgs", () => {
  it("omits --model for the inherited default and ends with an empty print prompt", () => {
    NodeAssert.deepEqual(buildJetskiArgs({ model: "default" }), [
      "--input-format",
      "stream-json",
      "--output-format",
      "stream-json",
      "-p=",
    ]);
  });

  it("orders mode, model, conversation and launch args before -p=", () => {
    NodeAssert.deepEqual(
      buildJetskiArgs({
        model: " gemini-pro ",
        conversationId: "abc",
        modeArgs: ["--mode", "auto"],
        launchArgs: ["--verbose"],
      }),
      [
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--mode",
        "auto",
        "--model",
        "gemini-pro",
        "--conversation",
        "abc",
        "--verbose",
        "-p=",
      ],
    );
  });
});

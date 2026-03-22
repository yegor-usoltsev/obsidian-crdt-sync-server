import { describe, expect, it } from "bun:test";
import {
  METADATA_PAYLOAD_LIMIT,
  parseControlMessage,
} from "../../src/transport/messages";

describe("control messages", () => {
  it("parses valid ping", () => {
    const result = parseControlMessage(JSON.stringify({ action: "ping" }));
    expect("error" in result).toBe(false);
    if (!("error" in result)) {
      expect(result.action).toBe("ping");
    }
  });

  it("parses valid metadata intent", () => {
    const msg = {
      action: "metadata.intent",
      payload: {
        type: "create",
        clientId: "c1",
        operationId: "op1",
        path: "test.md",
        kind: "text",
      },
    };
    const result = parseControlMessage(JSON.stringify(msg));
    expect("error" in result).toBe(false);
  });

  it("rejects invalid JSON", () => {
    const result = parseControlMessage("not json");
    expect("error" in result).toBe(true);
  });

  it("rejects unknown action", () => {
    const result = parseControlMessage(JSON.stringify({ action: "unknown" }));
    expect("error" in result).toBe(true);
  });

  it("rejects oversized payload", () => {
    const big = JSON.stringify({
      action: "ping",
      padding: "x".repeat(METADATA_PAYLOAD_LIMIT + 1),
    });
    const result = parseControlMessage(big);
    expect("error" in result).toBe(true);
  });

  it("parses diagnostics request", () => {
    const result = parseControlMessage(
      JSON.stringify({ action: "diagnostics.request" }),
    );
    expect("error" in result).toBe(false);
  });

  it("parses metadata subscribe", () => {
    const result = parseControlMessage(
      JSON.stringify({ action: "metadata.subscribe", sinceRevision: 5 }),
    );
    expect("error" in result).toBe(false);
  });

  it("parses history list", () => {
    const result = parseControlMessage(
      JSON.stringify({ action: "history.list", fileId: "abc" }),
    );
    expect("error" in result).toBe(false);
  });

  it("parses history restore", () => {
    const result = parseControlMessage(
      JSON.stringify({
        action: "history.restore",
        fileId: "abc",
        historyEntryId: 1,
      }),
    );
    expect("error" in result).toBe(false);
  });
});

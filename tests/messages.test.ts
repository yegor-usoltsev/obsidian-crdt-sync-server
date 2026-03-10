import { describe, expect, test } from "bun:test";
import {
  buildMetadataCommitMessage,
  buildMetadataRejectMessage,
  buildMetadataRejectMessageFromRaw,
  normalizeMetadataOpRequest,
} from "../src/messages.ts";

describe("normalizeMetadataOpRequest", () => {
  test("accepts the current file metadata protocol", () => {
    expect(
      normalizeMetadataOpRequest({
        type: "file.rename",
        fileId: "file-1",
        oldPath: "draft.md",
        newPath: "renamed.md",
        kind: "text",
      }),
    ).toEqual({
      type: "file.rename",
      fileId: "file-1",
      oldPath: "draft.md",
      newPath: "renamed.md",
      kind: "text",
    });
  });

  test("rejects removed legacy aliases", () => {
    expect(() =>
      normalizeMetadataOpRequest({
        type: "rename",
        docId: "file-1",
        oldPath: "draft.md",
        newPath: "renamed.md",
      }),
    ).toThrow('Unsupported metadata op type "rename"');

    expect(() =>
      normalizeMetadataOpRequest({
        type: "delete",
        docId: "file-1",
        filePath: "draft.md",
      }),
    ).toThrow('Unsupported metadata op type "delete"');
  });

  test("builds commit messages from the canonical metadata result", () => {
    expect(
      buildMetadataCommitMessage(
        {
          type: "file.rename",
          clientId: "client-1",
          clientOpId: "op-1",
          fileId: "file-1",
          oldPath: "draft.md",
          newPath: "renamed.md",
          kind: "text",
          timestamp: 123,
        },
        {
          eventId: 9,
          requestType: "file.rename",
          currentPath: "renamed.md",
          oldPath: "draft.md",
          newPath: "renamed.md",
          deduplicated: true,
        },
      ),
    ).toEqual({
      type: "metadata.commit",
      requestType: "file.rename",
      clientId: "client-1",
      clientOpId: "op-1",
      fileId: "file-1",
      metaEventId: 9,
      path: "renamed.md",
      oldPath: "draft.md",
      newPath: "renamed.md",
      kind: "text",
      timestamp: 123,
      deduplicated: true,
    });
  });

  test("builds reject messages with the attempted and canonical paths", () => {
    const reject = buildMetadataRejectMessage(
      {
        type: "file.rename",
        clientId: "client-1",
        clientOpId: "op-1",
        fileId: "file-1",
        oldPath: "draft.md",
        newPath: "renamed.md",
        kind: "text",
      },
      { path: "draft.md", kind: "text" },
      new Error("rename rejected"),
    );

    expect(reject).toMatchObject({
      type: "metadata.reject",
      requestType: "file.rename",
      clientId: "client-1",
      clientOpId: "op-1",
      fileId: "file-1",
      path: "renamed.md",
      oldPath: "draft.md",
      newPath: "renamed.md",
      kind: "text",
      currentPath: "draft.md",
      reason: "rename rejected",
    });
    expect(typeof reject.timestamp).toBe("number");
  });

  test("builds raw rejects only when correlation fields are present", () => {
    expect(
      buildMetadataRejectMessageFromRaw(
        {
          type: "file.create",
          clientId: "client-1",
          clientOpId: "op-1",
          fileId: "file-1",
          kind: "text",
        },
        "invalid payload",
      ),
    ).toMatchObject({
      type: "metadata.reject",
      requestType: "file.create",
      clientId: "client-1",
      clientOpId: "op-1",
      fileId: "file-1",
      kind: "text",
      reason: "invalid payload",
    });

    expect(
      buildMetadataRejectMessageFromRaw(
        {
          type: "file.create",
          clientId: "client-1",
        },
        "invalid payload",
      ),
    ).toBeNull();
  });
});

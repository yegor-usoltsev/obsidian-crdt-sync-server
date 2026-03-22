import type { Database } from "bun:sqlite";
import { beforeEach, describe, expect, it } from "bun:test";
import { createMetadataRegistry } from "../../src/metadata-registry/registry";
import { openDatabase } from "../../src/shared/database";

describe("metadata-registry", () => {
  let db: Database;
  let registry: ReturnType<typeof createMetadataRegistry>;

  beforeEach(() => {
    db = openDatabase(":memory:");
    registry = createMetadataRegistry(db);
  });

  describe("create", () => {
    it("creates a file with stable identity", () => {
      const result = registry.processIntent({
        type: "create",
        clientId: "client-1",
        operationId: "op-1",
        path: "notes/hello.md",
        kind: "text",
      });

      expect("fileId" in result).toBe(true);
      if ("fileId" in result) {
        expect(result.path).toBe("notes/hello.md");
        expect(result.kind).toBe("text");
        expect(result.deleted).toBe(false);
      }
    });

    it("rejects create without path", () => {
      const result = registry.processIntent({
        type: "create",
        clientId: "client-1",
        operationId: "op-2",
        kind: "text",
      });
      expect("reason" in result).toBe(true);
    });

    it("rejects case-insensitive path collision", () => {
      registry.processIntent({
        type: "create",
        clientId: "client-1",
        operationId: "op-1",
        path: "Notes/Hello.md",
        kind: "text",
      });

      const result = registry.processIntent({
        type: "create",
        clientId: "client-1",
        operationId: "op-2",
        path: "notes/hello.md",
        kind: "text",
      });
      expect("reason" in result).toBe(true);
      if ("reason" in result) {
        expect(result.reason).toContain("collision");
      }
    });

    it("rejects unsafe paths", () => {
      const result = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-unsafe",
        path: "../escape.txt",
        kind: "text",
      });
      expect("reason" in result).toBe(true);
    });

    it("rejects absolute paths", () => {
      const result = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-abs",
        path: "/etc/passwd",
        kind: "text",
      });
      expect("reason" in result).toBe(true);
    });
  });

  describe("rename", () => {
    it("renames preserving file identity", () => {
      const createResult = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-create",
        path: "old-name.md",
        kind: "text",
      });
      expect("fileId" in createResult).toBe(true);
      if (!("fileId" in createResult)) return;

      const renameResult = registry.processIntent({
        type: "rename",
        clientId: "c1",
        operationId: "op-rename",
        fileId: createResult.fileId,
        newPath: "new-name.md",
      });

      expect("fileId" in renameResult).toBe(true);
      if ("fileId" in renameResult) {
        expect(renameResult.fileId).toBe(createResult.fileId);
        expect(renameResult.path).toBe("new-name.md");
      }
    });

    it("renames directory and updates descendants", () => {
      // Create directory
      registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-dir",
        path: "folder",
        kind: "directory",
      });
      // Create child
      registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-child",
        path: "folder/note.md",
        kind: "text",
      });

      const dirFile = registry.getFileByPath("folder");
      expect(dirFile).not.toBeNull();

      const renameResult = registry.processIntent({
        type: "rename",
        clientId: "c1",
        operationId: "op-dir-rename",
        fileId: dirFile?.fileId,
        newPath: "renamed-folder",
      });

      expect("fileId" in renameResult).toBe(true);

      // Child should be updated
      const child = registry.getFileByPath("renamed-folder/note.md");
      expect(child).not.toBeNull();
    });

    it("rejects moving directory into own descendant", () => {
      registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-dir2",
        path: "parent",
        kind: "directory",
      });
      registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-child2",
        path: "parent/child",
        kind: "directory",
      });

      const dir = registry.getFileByPath("parent");
      const result = registry.processIntent({
        type: "move",
        clientId: "c1",
        operationId: "op-bad-move",
        fileId: dir?.fileId,
        newPath: "parent/child/parent",
      });
      expect("reason" in result).toBe(true);
    });
  });

  describe("delete", () => {
    it("marks file as deleted", () => {
      const createResult = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-del-create",
        path: "to-delete.md",
        kind: "text",
      });
      expect("fileId" in createResult).toBe(true);
      if (!("fileId" in createResult)) return;

      const deleteResult = registry.processIntent({
        type: "delete",
        clientId: "c1",
        operationId: "op-del",
        fileId: createResult.fileId,
      });

      expect("fileId" in deleteResult).toBe(true);
      if ("fileId" in deleteResult) {
        expect(deleteResult.deleted).toBe(true);
      }

      // Should not appear in active files
      const activeFiles = registry.listActiveFiles();
      expect(
        activeFiles.find((f) => f.fileId === createResult.fileId),
      ).toBeUndefined();
    });

    it("cascades directory delete to descendants", () => {
      registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-cascade-dir",
        path: "del-dir",
        kind: "directory",
      });
      registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-cascade-child",
        path: "del-dir/child.md",
        kind: "text",
      });

      const dir = registry.getFileByPath("del-dir");
      registry.processIntent({
        type: "delete",
        clientId: "c1",
        operationId: "op-cascade-del",
        fileId: dir?.fileId,
      });

      const activeFiles = registry.listActiveFiles();
      expect(
        activeFiles.find((f) => f.path.startsWith("del-dir")),
      ).toBeUndefined();
    });
  });

  describe("idempotent replay", () => {
    it("returns original result for replayed operation", () => {
      const first = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-replay",
        path: "replay-test.md",
        kind: "text",
      });

      const second = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-replay",
        path: "replay-test.md",
        kind: "text",
      });

      expect("fileId" in first).toBe(true);
      expect("fileId" in second).toBe(true);
      if ("fileId" in first && "fileId" in second) {
        expect(second.fileId).toBe(first.fileId);
        expect(second.revision).toBe(first.revision);
      }
    });
  });

  describe("content metadata", () => {
    it("updates advisory content metadata", () => {
      const result = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-content-create",
        path: "content-test.md",
        kind: "text",
      });
      expect("fileId" in result).toBe(true);
      if (!("fileId" in result)) return;

      const updated = registry.updateContentMetadata(
        result.fileId,
        "sha256-abc",
        1024,
        "c1",
      );
      expect(updated).not.toBeNull();
      expect(updated?.contentDigest).toBe("sha256-abc");
      expect(updated?.contentSize).toBe(1024);
      expect(updated?.contentAnchor).toBe(1);
    });

    it("advances content anchor on each update", () => {
      const result = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-anchor-create",
        path: "anchor-test.md",
        kind: "text",
      });
      if (!("fileId" in result)) return;

      registry.updateContentMetadata(result.fileId, "d1", 100, "c1");
      registry.updateContentMetadata(result.fileId, "d2", 200, "c1");
      const file = registry.getFile(result.fileId);
      expect(file?.contentAnchor).toBe(2);
    });

    it("returns null for deleted file", () => {
      const result = registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-del-content",
        path: "del-content.md",
        kind: "text",
      });
      if (!("fileId" in result)) return;

      registry.processIntent({
        type: "delete",
        clientId: "c1",
        operationId: "op-del-content-del",
        fileId: result.fileId,
      });

      const updated = registry.updateContentMetadata(
        result.fileId,
        "d",
        10,
        "c1",
      );
      expect(updated).toBeNull();
    });
  });

  describe("state", () => {
    it("returns epoch and revision", () => {
      const state = registry.getState();
      expect(state.epoch).toBeDefined();
      expect(typeof state.revision).toBe("number");
    });

    it("revision advances on operations", () => {
      const before = registry.getState().revision;
      registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: "op-rev-test",
        path: "rev-test.md",
        kind: "text",
      });
      const after = registry.getState().revision;
      expect(after).toBeGreaterThan(before);
    });
  });
});

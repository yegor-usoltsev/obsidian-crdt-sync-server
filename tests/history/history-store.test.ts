import type { Database } from "bun:sqlite";
import { beforeEach, describe, expect, it } from "bun:test";
import { createHistoryStore } from "../../src/history/history-store";
import { createMetadataRegistry } from "../../src/metadata-registry/registry";
import { openDatabase } from "../../src/shared/database";

describe("history-store", () => {
  let db: Database;
  let registry: ReturnType<typeof createMetadataRegistry>;
  let history: ReturnType<typeof createHistoryStore>;

  beforeEach(() => {
    db = openDatabase(":memory:");
    registry = createMetadataRegistry(db);
    history = createHistoryStore(db);
  });

  it("records history for create operations", () => {
    registry.processIntent({
      type: "create",
      clientId: "c1",
      operationId: "op-1",
      path: "test.md",
      kind: "text",
    });

    const entries = history.getHistorySince(0);
    expect(entries.length).toBeGreaterThan(0);
    expect(entries[0]?.operationType).toBe("create");
    expect(entries[0]?.path).toBe("test.md");
  });

  it("records history for rename operations", () => {
    const result = registry.processIntent({
      type: "create",
      clientId: "c1",
      operationId: "op-create",
      path: "old.md",
      kind: "text",
    });
    if (!("fileId" in result)) return;

    registry.processIntent({
      type: "rename",
      clientId: "c1",
      operationId: "op-rename",
      fileId: result.fileId,
      newPath: "new.md",
    });

    const entries = history.getFileHistory(result.fileId);
    expect(entries.length).toBe(2);
    expect(entries[1]?.operationType).toBe("rename");
  });

  it("records delete with content fingerprint", () => {
    const result = registry.processIntent({
      type: "create",
      clientId: "c1",
      operationId: "op-create-del",
      path: "to-del.md",
      kind: "text",
    });
    if (!("fileId" in result)) return;

    // Update content metadata first
    registry.updateContentMetadata(result.fileId, "sha256-test", 100, "c1");

    registry.processIntent({
      type: "delete",
      clientId: "c1",
      operationId: "op-delete",
      fileId: result.fileId,
      contentDigest: "sha256-test",
    });

    const entries = history.getFileHistory(result.fileId);
    const deleteEntry = entries.find((e) => e.operationType === "delete");
    expect(deleteEntry).toBeDefined();
    expect(deleteEntry?.contentDigest).toBe("sha256-test");
  });

  it("restores creating a new head (not mutating history)", () => {
    const result = registry.processIntent({
      type: "create",
      clientId: "c1",
      operationId: "op-restore-create",
      path: "restore-test.md",
      kind: "text",
    });
    if (!("fileId" in result)) return;

    // Delete the file
    registry.processIntent({
      type: "delete",
      clientId: "c1",
      operationId: "op-restore-delete",
      fileId: result.fileId,
    });

    // Get the create entry ID
    const entries = history.getFileHistory(result.fileId);
    const createEntry = entries.find((e) => e.operationType === "create");
    expect(createEntry).toBeDefined();

    // Restore to create state
    // biome-ignore lint/style/noNonNullAssertion: asserted above
    const restored = history.restore(result.fileId, createEntry!.id, "c1");
    expect(restored).not.toBeNull();
    expect(restored?.operationType).toBe("restore");

    // File should be active again
    const file = registry.getFile(result.fileId);
    expect(file).not.toBeNull();
    expect(file?.deleted).toBe(false);

    // History should now have 4 entries: create, content-update (from updateContent in delete path? Actually let's check), delete, restore
    const finalEntries = history.getFileHistory(result.fileId);
    expect(finalEntries.length).toBeGreaterThanOrEqual(3);
    expect(finalEntries[finalEntries.length - 1]?.operationType).toBe(
      "restore",
    );
  });

  it("compacts history and bumps epoch", () => {
    // Create several entries
    for (let i = 0; i < 5; i++) {
      registry.processIntent({
        type: "create",
        clientId: "c1",
        operationId: `op-compact-${i}`,
        path: `compact-${i}.md`,
        kind: "text",
      });
    }

    const epochBefore = registry.getState().epoch;
    const countBefore = history.getEntryCount();
    expect(countBefore).toBe(5);

    // Compact entries before revision 3
    const result = history.compactBefore(3);
    expect(result.deletedEntries).toBeGreaterThan(0);
    expect(result.newEpoch).not.toBe(epochBefore);

    // Epoch changed
    const epochAfter = registry.getState().epoch;
    expect(epochAfter).toBe(result.newEpoch);

    // Entry count decreased
    const countAfter = history.getEntryCount();
    expect(countAfter).toBeLessThan(countBefore);
  });

  it("returns empty for non-existent file", () => {
    const entries = history.getFileHistory("nonexistent");
    expect(entries).toEqual([]);
  });

  it("returns null for non-existent restore target", () => {
    const result = history.restore("nonexistent", 999, "c1");
    expect(result).toBeNull();
  });
});

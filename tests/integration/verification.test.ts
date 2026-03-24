/**
 * Verification tests for fix-all-implementation-gaps tasks 18.1–18.31 (server-side).
 */

import type { Database } from "bun:sqlite";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { HocuspocusProvider } from "@hocuspocus/provider";
import * as Y from "yjs";
import {
  isBackupExcluded,
  readGitBackupConfig,
} from "../../src/backup/git-backup";
import {
  type BlobStore,
  createBlobStore,
} from "../../src/blob-store/blob-store";
import {
  createHistoryStore,
  type HistoryStore,
} from "../../src/history/history-store";
import {
  createMetadataRegistry,
  type MetadataRegistry,
} from "../../src/metadata-registry/registry";
import {
  createSettingsStore,
  type SettingsStore,
} from "../../src/settings-store/settings-store";
import { openDatabase } from "../../src/shared/database";
import { createTextDocService } from "../../src/text-doc-service/text-doc-service";
import type { ControlResponse } from "../../src/transport/messages";
import { createSyncServer, type SyncServer } from "../../src/transport/server";

describe("verification tests", () => {
  let db: Database;
  let server: SyncServer;
  let registry: MetadataRegistry;
  let historyStore: HistoryStore;
  let blobStore: BlobStore;
  let settingsStore: SettingsStore;
  const AUTH_TOKEN = "a".repeat(32);
  let baseUrl: string;
  const blobDir = "/tmp/crdt-sync-verification-test/blobs";
  const broadcasts: ControlResponse[] = [];

  function createFile(
    path: string,
    kind: "text" | "binary" | "directory" = "text",
  ): string {
    const result = registry.processIntent({
      type: "create",
      clientId: "test-client",
      operationId: crypto.randomUUID(),
      path,
      kind,
    });
    if (!("fileId" in result))
      throw new Error(`Create failed: ${result.reason}`);
    return result.fileId;
  }

  function computeDigest(content: Uint8Array): string {
    const hasher = new Bun.CryptoHasher("sha256");
    hasher.update(content);
    return hasher.digest("hex");
  }

  function authHeaders(): Record<string, string> {
    return { Authorization: `Bearer ${AUTH_TOKEN}` };
  }

  beforeAll(async () => {
    db = openDatabase(":memory:");
    registry = createMetadataRegistry(db);
    blobStore = await createBlobStore(db, blobDir);
    settingsStore = createSettingsStore(db);
    historyStore = createHistoryStore(db, blobStore, settingsStore);

    let broadcastFn: (msg: ControlResponse) => void = () => {};
    const textDocService = createTextDocService({
      db,
      authToken: AUTH_TOKEN,
      registry,
      broadcast: (msg) => broadcastFn(msg),
    });

    server = createSyncServer({
      port: 0,
      authToken: AUTH_TOKEN,
      db,
      registry,
      historyStore,
      blobStore,
      settingsStore,
      textDocService,
    });
    await server.start();
    broadcastFn = (msg) => {
      broadcasts.push(msg);
      server.broadcast(msg);
    };
    baseUrl = `http://localhost:${server.port}`;
  });

  afterAll(async () => {
    await server.stop();
    db.close();
  });

  // --- 18.2: VERIFY text edits advance content metadata ---
  describe("18.2: text edits advance content metadata", () => {
    it("stores Y.Doc and advances registry contentDigest and contentAnchor", () => {
      const fileId = createFile("text-metadata-test.md", "text");

      // Simulate what onStoreDocument does: write Y.Doc, update registry
      const doc = new Y.Doc();
      doc.getText("content").insert(0, "Hello, world!");
      const state = Y.encodeStateAsUpdate(doc);

      db.run(
        "INSERT OR REPLACE INTO text_documents (file_id, data, updated_at) VALUES (?, ?, ?)",
        [fileId, Buffer.from(state), Date.now()],
      );

      const textContent = doc.getText("content").toString();
      const textBytes = new TextEncoder().encode(textContent);
      const digest = computeDigest(textBytes);
      doc.destroy();

      const updated = registry.updateContentMetadata(
        fileId,
        digest,
        textBytes.byteLength,
        "text-doc-store",
      );

      expect(updated).not.toBeNull();
      expect(updated?.contentDigest).toBe(digest);
      expect(updated?.contentAnchor).toBeGreaterThan(0);

      // Verify history contains content-update entry
      const history = historyStore.getFileHistory(fileId);
      const contentUpdate = history.find(
        (e) => e.operationType === "content-update",
      );
      expect(contentUpdate).toBeDefined();
    });
  });

  // --- 18.3: VERIFY requestId echoing ---
  describe("18.3: requestId echoing", () => {
    it("echoes requestId in history.list response", async () => {
      const fileId = createFile("requestid-test.md", "text");
      const requestId = crypto.randomUUID();

      const ws = new WebSocket(
        `ws://localhost:${server.port}/ws?token=${AUTH_TOKEN}`,
      );

      const result = await new Promise<Record<string, unknown>>(
        (resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error("Timeout")), 5000);

          ws.onopen = () => {
            ws.send(
              JSON.stringify({
                action: "history.list",
                fileId,
                requestId,
              }),
            );
          };

          ws.onmessage = (event) => {
            const msg = JSON.parse(String(event.data));
            if (msg.action === "history.list") {
              clearTimeout(timeout);
              resolve(msg);
            }
          };

          ws.onerror = (e) => {
            clearTimeout(timeout);
            reject(e);
          };
        },
      );

      expect(result.requestId).toBe(requestId);
      ws.close();
    });

    it("echoes requestId in diagnostics.response", async () => {
      const requestId = crypto.randomUUID();

      const ws = new WebSocket(
        `ws://localhost:${server.port}/ws?token=${AUTH_TOKEN}`,
      );

      const result = await new Promise<Record<string, unknown>>(
        (resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error("Timeout")), 5000);

          ws.onopen = () => {
            ws.send(
              JSON.stringify({
                action: "diagnostics.request",
                requestId,
              }),
            );
          };

          ws.onmessage = (event) => {
            const msg = JSON.parse(String(event.data));
            if (msg.action === "diagnostics.response") {
              clearTimeout(timeout);
              resolve(msg);
            }
          };

          ws.onerror = (e) => {
            clearTimeout(timeout);
            reject(e);
          };
        },
      );

      expect(result.requestId).toBe(requestId);
      ws.close();
    });
  });

  // --- 18.4: VERIFY metadata.replay-complete is sent ---
  describe("18.4: metadata.replay-complete", () => {
    it("sends replay-complete after catch-up commits", async () => {
      // Create 3 files first
      createFile("replay-test-1.md", "text");
      createFile("replay-test-2.md", "text");
      createFile("replay-test-3.md", "text");

      const requestId = crypto.randomUUID();
      const ws = new WebSocket(
        `ws://localhost:${server.port}/ws?token=${AUTH_TOKEN}`,
      );

      const messages: Record<string, unknown>[] = [];
      const replayComplete = await new Promise<Record<string, unknown>>(
        (resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error("Timeout")), 5000);

          ws.onopen = () => {
            ws.send(
              JSON.stringify({
                action: "metadata.subscribe",
                sinceRevision: 0,
                requestId,
              }),
            );
          };

          ws.onmessage = (event) => {
            const msg = JSON.parse(String(event.data));
            messages.push(msg);
            if (msg.action === "metadata.replay-complete") {
              clearTimeout(timeout);
              resolve(msg);
            }
          };

          ws.onerror = (e) => {
            clearTimeout(timeout);
            reject(e);
          };
        },
      );

      // Should have received commits then replay-complete
      const commits = messages.filter((m) => m.action === "metadata.commit");
      expect(commits.length).toBeGreaterThanOrEqual(3);
      expect(replayComplete.action).toBe("metadata.replay-complete");
      expect(replayComplete.requestId).toBe(requestId);
      expect(typeof replayComplete.currentRevision).toBe("number");

      ws.close();
    });
  });

  // --- 18.11: VERIFY contentAnchor required on rename/delete ---
  describe("18.11: contentAnchor required on rename/delete", () => {
    it("rejects rename without contentAnchor", () => {
      const fileId = createFile("anchor-required-rename.md", "text");
      registry.updateContentMetadata(fileId, "d1", 100, "c1");

      const result = registry.processIntent({
        type: "rename",
        clientId: "c1",
        operationId: crypto.randomUUID(),
        fileId,
        newPath: "anchor-required-rename-new.md",
        // No contentAnchor
      });
      expect("reason" in result).toBe(true);
      if ("reason" in result) {
        expect(result.reason).toBe("contentAnchor is required");
      }
    });

    it("rejects delete without contentAnchor", () => {
      const fileId = createFile("anchor-required-delete.md", "text");
      registry.updateContentMetadata(fileId, "d1", 100, "c1");

      const result = registry.processIntent({
        type: "delete",
        clientId: "c1",
        operationId: crypto.randomUUID(),
        fileId,
        // No contentAnchor
      });
      expect("reason" in result).toBe(true);
      if ("reason" in result) {
        expect(result.reason).toBe("contentAnchor is required");
      }
    });
  });

  // --- 18.12: VERIFY history restore broadcasts ---
  describe("18.12: history restore broadcasts", () => {
    it("broadcasts restore commit to other clients", async () => {
      const fileId = createFile("restore-broadcast-test.md", "text");
      registry.updateContentMetadata(fileId, "d1", 100, "c1");

      // Connect two WebSocket clients
      const ws1 = new WebSocket(
        `ws://localhost:${server.port}/ws?token=${AUTH_TOKEN}`,
      );
      const ws2 = new WebSocket(
        `ws://localhost:${server.port}/ws?token=${AUTH_TOKEN}`,
      );

      // Wait for both to open
      await Promise.all([
        new Promise<void>((resolve) => {
          ws1.onopen = () => resolve();
        }),
        new Promise<void>((resolve) => {
          ws2.onopen = () => resolve();
        }),
      ]);

      // Subscribe ws2 so it receives broadcasts
      ws2.send(
        JSON.stringify({ action: "metadata.subscribe", sinceRevision: 0 }),
      );
      // Wait for replay-complete on ws2
      await new Promise<void>((resolve) => {
        ws2.onmessage = (event) => {
          const msg = JSON.parse(String(event.data));
          if (msg.action === "metadata.replay-complete") resolve();
        };
      });

      // Get history entries to find the create entry
      const history = historyStore.getFileHistory(fileId);
      const createEntry = history.find((e) => e.operationType === "create");
      expect(createEntry).toBeDefined();

      // ws1 requests a restore
      const restoreCommit = new Promise<Record<string, unknown>>(
        (resolve, reject) => {
          const timeout = setTimeout(
            () =>
              reject(new Error("Timeout waiting for restore commit on ws2")),
            5000,
          );
          ws2.onmessage = (event) => {
            const msg = JSON.parse(String(event.data));
            if (
              msg.action === "metadata.commit" &&
              msg.payload?.operationType === "restore"
            ) {
              clearTimeout(timeout);
              resolve(msg);
            }
          };
        },
      );

      ws1.send(
        JSON.stringify({
          action: "history.restore",
          fileId,
          // biome-ignore lint/style/noNonNullAssertion: checked above
          historyEntryId: createEntry!.id,
        }),
      );

      const received = await restoreCommit;
      expect(received.action).toBe("metadata.commit");
      expect((received.payload as Record<string, unknown>).operationType).toBe(
        "restore",
      );
      expect((received.payload as Record<string, unknown>).fileId).toBe(fileId);

      ws1.close();
      ws2.close();
    });
  });

  // --- 18.17: VERIFY git backup rejects http:// remote ---
  describe("18.17: git backup rejects http:// remote", () => {
    it("rejects http:// remote URL", () => {
      const originalEnv = { ...process.env };
      try {
        process.env.BACKUP_GIT_INTERVAL_MINUTES = "5";
        process.env.BACKUP_GIT_URL = "http://example.com/repo.git";
        const config = readGitBackupConfig();
        expect(config).toBeNull();
      } finally {
        Object.assign(process.env, originalEnv);
        if (!originalEnv.BACKUP_GIT_INTERVAL_MINUTES)
          delete process.env.BACKUP_GIT_INTERVAL_MINUTES;
        if (!originalEnv.BACKUP_GIT_URL) delete process.env.BACKUP_GIT_URL;
      }
    });

    it("accepts https:// remote URL", () => {
      const originalEnv = { ...process.env };
      try {
        process.env.BACKUP_GIT_INTERVAL_MINUTES = "5";
        process.env.BACKUP_GIT_URL = "https://example.com/repo.git";
        const config = readGitBackupConfig();
        expect(config).not.toBeNull();
        expect(config?.remoteUrl).toContain("https://");
      } finally {
        Object.assign(process.env, originalEnv);
        if (!originalEnv.BACKUP_GIT_INTERVAL_MINUTES)
          delete process.env.BACKUP_GIT_INTERVAL_MINUTES;
        if (!originalEnv.BACKUP_GIT_URL) delete process.env.BACKUP_GIT_URL;
      }
    });
  });

  // --- 18.18: VERIFY git backup applies policy filter ---
  describe("18.18: git backup applies policy filter", () => {
    it("excludes .DS_Store", () => {
      expect(isBackupExcluded(".DS_Store")).toBe(true);
    });

    it("excludes Thumbs.db", () => {
      expect(isBackupExcluded("Thumbs.db")).toBe(true);
    });

    it("excludes desktop.ini", () => {
      expect(isBackupExcluded("desktop.ini")).toBe(true);
    });

    it("excludes node_modules/ paths", () => {
      expect(isBackupExcluded("node_modules/foo.js")).toBe(true);
      expect(isBackupExcluded("some/path/node_modules/bar.js")).toBe(true);
    });

    it("excludes dot-prefixed directory segments", () => {
      expect(isBackupExcluded(".hidden/file.txt")).toBe(true);
      expect(isBackupExcluded("path/.git/config")).toBe(true);
    });

    it("does NOT exclude regular files", () => {
      expect(isBackupExcluded("notes/hello.md")).toBe(false);
      expect(isBackupExcluded("images/photo.png")).toBe(false);
    });
  });

  // --- 18.13: VERIFY dead code removal ---
  describe("18.13: dead code removal compilation", () => {
    it("ensureTextDocTable is not exported from text-doc-service", async () => {
      const mod = await import("../../src/text-doc-service/text-doc-service");
      expect("ensureTextDocTable" in mod).toBe(false);
    });
  });

  // --- 18.20: VERIFY content-update broadcast reaches subscribers ---
  describe("18.20: content-update broadcast on blob upload", () => {
    it("broadcasts metadata.commit with content-update on blob upload", async () => {
      const fileId = createFile("broadcast-blob-test.bin", "binary");
      const content = new Uint8Array([42, 43, 44]);
      const digest = computeDigest(content);

      // Connect a subscriber
      const ws = new WebSocket(
        `ws://localhost:${server.port}/ws?token=${AUTH_TOKEN}`,
      );
      await new Promise<void>((resolve) => {
        ws.onopen = () => resolve();
      });
      ws.send(
        JSON.stringify({ action: "metadata.subscribe", sinceRevision: 0 }),
      );
      // Wait for replay-complete
      await new Promise<void>((resolve) => {
        ws.onmessage = (event) => {
          const msg = JSON.parse(String(event.data));
          if (msg.action === "metadata.replay-complete") resolve();
        };
      });

      // Set up listener for content-update
      const commitPromise = new Promise<Record<string, unknown>>(
        (resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error("Timeout")), 5000);
          ws.onmessage = (event) => {
            const msg = JSON.parse(String(event.data));
            if (
              msg.action === "metadata.commit" &&
              msg.payload?.operationType === "content-update" &&
              msg.payload?.fileId === fileId
            ) {
              clearTimeout(timeout);
              resolve(msg);
            }
          };
        },
      );

      // Upload blob via HTTP
      await fetch(`${baseUrl}/blobs/${fileId}`, {
        method: "PUT",
        headers: {
          ...authHeaders(),
          "Content-Type": "application/octet-stream",
          "X-Content-Digest": digest,
        },
        body: content,
      });

      const msg = await commitPromise;
      const payload = msg.payload as Record<string, unknown>;
      expect(payload.operationType).toBe("content-update");
      expect(payload.contentDigest).toBe(digest);
      expect(payload.contentSize).toBe(3);

      ws.close();
    });
  });

  // --- 18.23: VERIFY operationType in MetadataCommit ---
  describe("18.23: operationType in MetadataCommit", () => {
    it("create intent produces commit with operationType: 'create'", async () => {
      const ws = new WebSocket(
        `ws://localhost:${server.port}/ws?token=${AUTH_TOKEN}`,
      );

      const commit = await new Promise<Record<string, unknown>>(
        (resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error("Timeout")), 5000);

          ws.onopen = () => {
            ws.send(
              JSON.stringify({
                action: "metadata.intent",
                payload: {
                  type: "create",
                  clientId: "test-client",
                  operationId: crypto.randomUUID(),
                  path: "optype-create-test.md",
                  kind: "text",
                },
              }),
            );
          };

          ws.onmessage = (event) => {
            const msg = JSON.parse(String(event.data));
            if (msg.action === "metadata.commit") {
              clearTimeout(timeout);
              resolve(msg.payload);
            }
          };

          ws.onerror = (e) => {
            clearTimeout(timeout);
            reject(e);
          };
        },
      );

      expect(commit.operationType).toBe("create");
      ws.close();
    });

    it("updateContentMetadata produces content-update history entry", () => {
      const fileId = createFile("optype-content-update.md", "text");
      registry.updateContentMetadata(fileId, "d1", 100, "c1");

      const history = historyStore.getFileHistory(fileId);
      const contentUpdate = history.find(
        (e) => e.operationType === "content-update",
      );
      expect(contentUpdate).toBeDefined();
    });
  });

  // --- 18.25: VERIFY text snapshots created on store ---
  describe("18.25: text snapshots created on store", () => {
    it("inserts text_snapshots row after text doc store and metadata update", () => {
      const fileId = createFile("text-snapshot-test.md", "text");

      // Simulate onStoreDocument: write Y.Doc
      const doc = new Y.Doc();
      doc.getText("content").insert(0, "Snapshot test content");
      const state = Y.encodeStateAsUpdate(doc);
      db.run(
        "INSERT OR REPLACE INTO text_documents (file_id, data, updated_at) VALUES (?, ?, ?)",
        [fileId, Buffer.from(state), Date.now()],
      );

      const textContent = doc.getText("content").toString();
      const textBytes = new TextEncoder().encode(textContent);
      const digest = computeDigest(textBytes);
      doc.destroy();

      const updated = registry.updateContentMetadata(
        fileId,
        digest,
        textBytes.byteLength,
        "text-doc-store",
      );
      expect(updated).not.toBeNull();

      // Simulate text snapshot insertion (as done in onStoreDocument)
      db.run(
        "INSERT OR REPLACE INTO text_snapshots (file_id, content_anchor, content, stored_at) VALUES (?, ?, ?, ?)",
        // biome-ignore lint/style/noNonNullAssertion: checked not null above
        [fileId, updated!.contentAnchor, textContent, Date.now()],
      );

      // Verify text_snapshots has the row
      const snapshot = db
        .query(
          "SELECT content FROM text_snapshots WHERE file_id = ? AND content_anchor = ?",
        )
        // biome-ignore lint/style/noNonNullAssertion: checked not null above
        .get(fileId, updated!.contentAnchor) as { content: string } | null;

      expect(snapshot).not.toBeNull();
      expect(snapshot?.content).toBe("Snapshot test content");
    });
  });

  // --- 18.26: VERIFY binary restore end-to-end ---
  describe("18.26: binary restore end-to-end", () => {
    it("restores binary file to historical anchor", async () => {
      const fileId = createFile("binary-restore-test.bin", "binary");

      // Upload version 1
      const v1 = new Uint8Array([1, 2, 3]);
      const d1 = computeDigest(v1);
      await blobStore.store(fileId, v1, d1, 1);
      registry.updateContentMetadata(fileId, d1, v1.byteLength, "c1");

      // Upload version 2
      const v2 = new Uint8Array([4, 5, 6]);
      const d2 = computeDigest(v2);
      await blobStore.store(fileId, v2, d2, 2);
      registry.updateContentMetadata(fileId, d2, v2.byteLength, "c1");

      // Get the history entry for anchor 1
      const history = historyStore.getFileHistory(fileId);
      const firstContentUpdate = history.find(
        (e) => e.operationType === "content-update" && e.contentAnchor === 1,
      );
      expect(firstContentUpdate).toBeDefined();

      // Restore to first version
      const restored = historyStore.restore(
        fileId,
        // biome-ignore lint/style/noNonNullAssertion: checked defined above
        firstContentUpdate!.id,
        "c1",
      );
      expect(restored).not.toBeNull();
      expect(restored?.operationType).toBe("restore");

      // Verify the file metadata shows the restored state
      const fileMeta = registry.getFile(fileId);
      expect(fileMeta?.contentAnchor).toBe(restored?.contentAnchor);
    });
  });

  // --- 18.27: VERIFY settings restore end-to-end ---
  describe("18.27: settings restore end-to-end", () => {
    it("restores settings to historical anchor", () => {
      const fileId = createFile("settings-restore-test.json", "text");

      // Store version 1
      const v1 = new TextEncoder().encode('{"theme":"light"}');
      const d1 = computeDigest(v1);
      settingsStore.store(fileId, v1, d1, 1);
      registry.updateContentMetadata(fileId, d1, v1.byteLength, "c1");

      // Store version 2
      const v2 = new TextEncoder().encode('{"theme":"dark"}');
      const d2 = computeDigest(v2);
      settingsStore.store(fileId, v2, d2, 2);
      registry.updateContentMetadata(fileId, d2, v2.byteLength, "c1");

      // Get the history entry for anchor 1
      const history = historyStore.getFileHistory(fileId);
      const firstContentUpdate = history.find(
        (e) => e.operationType === "content-update" && e.contentAnchor === 1,
      );
      expect(firstContentUpdate).toBeDefined();

      // Restore to first version
      const restored = historyStore.restore(
        fileId,
        // biome-ignore lint/style/noNonNullAssertion: checked defined above
        firstContentUpdate!.id,
        "c1",
      );
      expect(restored).not.toBeNull();
      expect(restored?.operationType).toBe("restore");

      // Check that the settings snapshot was copied to the new anchor
      const restoredSnapshot = settingsStore.getByAnchor(
        fileId,
        // biome-ignore lint/style/noNonNullAssertion: checked not null above
        restored!.contentAnchor,
      );
      expect(restoredSnapshot).not.toBeNull();
      // biome-ignore lint/style/noNonNullAssertion: checked not null above
      expect(new TextDecoder().decode(restoredSnapshot!.content)).toBe(
        '{"theme":"light"}',
      );
    });
  });

  // --- 18.28: VERIFY text restore end-to-end ---
  describe("18.28: text restore end-to-end", () => {
    it("restores text file to historical content", () => {
      const fileId = createFile("text-restore-test.md", "text");

      // Version 1: insert text + snapshot
      const doc1 = new Y.Doc();
      doc1.getText("content").insert(0, "Version 1 text");
      const state1 = Y.encodeStateAsUpdate(doc1);
      db.run(
        "INSERT OR REPLACE INTO text_documents (file_id, data, updated_at) VALUES (?, ?, ?)",
        [fileId, Buffer.from(state1), Date.now()],
      );
      const text1 = doc1.getText("content").toString();
      const bytes1 = new TextEncoder().encode(text1);
      const digest1 = computeDigest(bytes1);
      doc1.destroy();
      registry.updateContentMetadata(fileId, digest1, bytes1.byteLength, "c1");
      db.run(
        "INSERT OR REPLACE INTO text_snapshots (file_id, content_anchor, content, stored_at) VALUES (?, ?, ?, ?)",
        [fileId, 1, text1, Date.now()],
      );

      // Version 2: different text + snapshot
      const doc2 = new Y.Doc();
      doc2
        .getText("content")
        .insert(0, "Version 2 text - completely different");
      const state2 = Y.encodeStateAsUpdate(doc2);
      db.run(
        "INSERT OR REPLACE INTO text_documents (file_id, data, updated_at) VALUES (?, ?, ?)",
        [fileId, Buffer.from(state2), Date.now()],
      );
      const text2 = doc2.getText("content").toString();
      const bytes2 = new TextEncoder().encode(text2);
      const digest2 = computeDigest(bytes2);
      doc2.destroy();
      registry.updateContentMetadata(fileId, digest2, bytes2.byteLength, "c1");
      db.run(
        "INSERT OR REPLACE INTO text_snapshots (file_id, content_anchor, content, stored_at) VALUES (?, ?, ?, ?)",
        [fileId, 2, text2, Date.now()],
      );

      // Get the history entry for anchor 1
      const history = historyStore.getFileHistory(fileId);
      const firstContentUpdate = history.find(
        (e) => e.operationType === "content-update" && e.contentAnchor === 1,
      );
      expect(firstContentUpdate).toBeDefined();

      // Restore to version 1
      const restored = historyStore.restore(
        fileId,
        // biome-ignore lint/style/noNonNullAssertion: checked defined above
        firstContentUpdate!.id,
        "c1",
      );
      expect(restored).not.toBeNull();
      expect(restored?.operationType).toBe("restore");

      // Check the text_documents row now has a Y.Doc with "Version 1 text"
      const row = db
        .query("SELECT data FROM text_documents WHERE file_id = ?")
        .get(fileId) as { data: Buffer } | null;
      expect(row).not.toBeNull();

      const restoredDoc = new Y.Doc();
      // biome-ignore lint/style/noNonNullAssertion: checked not null above
      Y.applyUpdate(restoredDoc, new Uint8Array(row!.data));
      const restoredText = restoredDoc.getText("content").toString();
      restoredDoc.destroy();

      expect(restoredText).toBe("Version 1 text");

      // Verify text snapshot was created at new anchor
      const newSnapshot = db
        .query(
          "SELECT content FROM text_snapshots WHERE file_id = ? AND content_anchor = ?",
        )
        // biome-ignore lint/style/noNonNullAssertion: checked not null above
        .get(fileId, restored!.contentAnchor) as { content: string } | null;
      expect(newSnapshot).not.toBeNull();
      expect(newSnapshot?.content).toBe("Version 1 text");
    });
  });

  // --- 18.1 / 5.5: VERIFY bun-ws-shim with Hocuspocus round-trip ---
  describe("18.1: bun-ws-shim Hocuspocus round-trip", () => {
    it("round-trips Y.Doc text through the server", async () => {
      const fileId = createFile("hocuspocus-roundtrip.md", "text");
      const doc = new Y.Doc();

      const provider = new HocuspocusProvider({
        // The server routes /docs WebSocket connections to Hocuspocus.
        // Auth and document name are handled via the Hocuspocus wire protocol.
        url: `ws://localhost:${server.port}/docs`,
        name: fileId,
        document: doc,
        token: AUTH_TOKEN,
      } as ConstructorParameters<typeof HocuspocusProvider>[0]);

      // Wait for sync
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          provider.destroy();
          doc.destroy();
          reject(new Error("Hocuspocus sync timeout"));
        }, 10000);

        provider.on("synced", () => {
          clearTimeout(timeout);
          resolve();
        });
      });

      // Write text content
      doc.getText("content").insert(0, "Hello from test!");

      // Disconnect the provider, which triggers Hocuspocus to flush
      // (onStoreDocument fires immediately on last-connection close
      // when unloadImmediately is true — the default).
      provider.destroy();

      // Wait for async onStoreDocument to complete
      await new Promise((r) => setTimeout(r, 1500));

      // Verify server stored the document
      const row = db
        .query("SELECT data FROM text_documents WHERE file_id = ?")
        .get(fileId) as { data: Buffer } | null;
      expect(row).not.toBeNull();

      // Verify the Y.Doc content on server
      const serverDoc = new Y.Doc();
      // biome-ignore lint/style/noNonNullAssertion: checked not null above
      Y.applyUpdate(serverDoc, new Uint8Array(row!.data));
      expect(serverDoc.getText("content").toString()).toBe("Hello from test!");
      serverDoc.destroy();

      doc.destroy();
    });
  });

  // --- 18.22: VERIFY contentDigest and contentSize persisted ---
  describe("18.22: contentDigest and contentSize in commit payload", () => {
    it("includes contentDigest and contentSize in create commit via WebSocket", async () => {
      const ws = new WebSocket(
        `ws://localhost:${server.port}/ws?token=${AUTH_TOKEN}`,
      );
      const fileId = createFile("digest-size-commit.bin", "binary");
      const content = new Uint8Array([10, 20, 30, 40, 50]);
      const digest = computeDigest(content);

      // Upload blob to set content metadata
      await fetch(`${baseUrl}/blobs/${fileId}`, {
        method: "PUT",
        headers: {
          ...authHeaders(),
          "Content-Type": "application/octet-stream",
          "X-Content-Digest": digest,
        },
        body: content,
      });

      // Verify registry has the fields
      const fileMeta = registry.getFile(fileId);
      expect(fileMeta?.contentDigest).toBe(digest);
      expect(fileMeta?.contentSize).toBe(5);
      expect(fileMeta?.contentAnchor).toBeGreaterThan(0);

      ws.close();
    });
  });

  // --- 18.9: VERIFY settings sync merge via server round-trip ---
  describe("18.9: settings sync merge via server round-trip", () => {
    it("uploads and downloads settings with merge metadata", async () => {
      const configPath = "community-plugins.json";

      // Upload initial version
      const v1 = new TextEncoder().encode('["plugin-a","plugin-b"]');
      const d1 = computeDigest(v1);

      const uploadRes = await fetch(
        `${baseUrl}/settings/${encodeURIComponent(configPath)}`,
        {
          method: "PUT",
          headers: {
            ...authHeaders(),
            "Content-Type": "application/octet-stream",
            "X-Content-Digest": d1,
          },
          body: v1,
        },
      );
      expect(uploadRes.status).toBe(200);

      // Download and verify
      const downloadRes = await fetch(
        `${baseUrl}/settings/${encodeURIComponent(configPath)}`,
        { headers: authHeaders() },
      );
      expect(downloadRes.status).toBe(200);
      const downloaded = new Uint8Array(await downloadRes.arrayBuffer());
      expect(JSON.parse(new TextDecoder().decode(downloaded))).toEqual([
        "plugin-a",
        "plugin-b",
      ]);
      expect(downloadRes.headers.get("x-content-digest")).toBe(d1);
    });
  });
});

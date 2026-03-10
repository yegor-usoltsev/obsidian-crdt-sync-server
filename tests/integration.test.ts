import { Database as BunSQLite } from "bun:sqlite";
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import {
  HocuspocusProvider,
  HocuspocusProviderWebsocket,
} from "@hocuspocus/provider";
import { $ } from "bun";
import { join } from "pathe";
import * as Y from "yjs";
import { CONTENT_DOCUMENT_NAME, META_DOCUMENT_NAME } from "../src/meta-doc.ts";
import { createSyncServer } from "../src/server.ts";

const TEST_TOKEN = "test-token";
const TIMEOUT = 8_000;
const TEMP_DIR = Bun.env.TMPDIR || "/tmp";

function getFreePort(): Promise<number> {
  const listener = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: { data() {} },
  });
  const { port } = listener;
  listener.stop(true);
  return Promise.resolve(port);
}

async function createTempDir(prefix: string): Promise<string> {
  const dir = join(TEMP_DIR, `${prefix}${Bun.randomUUIDv7()}`);
  await Bun.write(join(dir, ".tmp"), "");
  await Bun.file(join(dir, ".tmp")).delete();
  return dir;
}

async function startTestServer(existingDataDir?: string): Promise<{
  server: Awaited<ReturnType<typeof createSyncServer>>;
  url: string;
  httpUrl: string;
  dataDir: string;
}> {
  const port = await getFreePort();
  const dataDir =
    existingDataDir ?? (await createTempDir("crdt-sync-server-phase-2-test-"));
  const server = await createSyncServer({
    authToken: TEST_TOKEN,
    dataDir,
  });

  await server.listen(port);

  return {
    server,
    url: `ws://127.0.0.1:${port}`,
    httpUrl: `http://127.0.0.1:${port}`,
    dataDir,
  };
}

function waitFor(condition: () => boolean, timeout = TIMEOUT): Promise<void> {
  if (condition()) {
    return Promise.resolve();
  }

  return new Promise<void>((resolve, reject) => {
    const startedAt = Date.now();
    const timer = setInterval(() => {
      if (condition()) {
        clearInterval(timer);
        resolve();
        return;
      }

      if (Date.now() - startedAt >= timeout) {
        clearInterval(timer);
        reject(new Error(`Timed out after ${timeout}ms`));
      }
    }, 25);
  });
}

interface MetaClient {
  provider: HocuspocusProvider;
  ydoc: Y.Doc;
  metaFiles: Y.Map<Y.Map<unknown>>;
  metaEvents: Y.Array<Y.Map<unknown>>;
  metaServerState: Y.Map<unknown>;
  statelessMessages: Array<Record<string, unknown>>;
  waitForSync: (timeout?: number) => Promise<void>;
  waitForStateless: (
    predicate: (message: Record<string, unknown>) => boolean,
    timeout?: number,
  ) => Promise<Record<string, unknown>>;
  sendStateless: (message: Record<string, unknown>) => void;
  destroy: () => void;
}

function createSyncWaiter(
  provider: HocuspocusProvider,
  syncedResolvers: Array<() => void>,
): (timeout?: number) => Promise<void> {
  return (timeout = TIMEOUT) => {
    if (provider.isSynced) {
      return Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
      const onSync = () => {
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(() => {
        const index = syncedResolvers.indexOf(onSync);
        if (index !== -1) {
          syncedResolvers.splice(index, 1);
        }
        reject(new Error(`Sync timeout after ${timeout}ms`));
      }, timeout);

      syncedResolvers.push(onSync);
    });
  };
}

function createMetaClient(
  url: string,
  options: {
    token?: string | null;
    onAuthFailed?: () => void;
  } = {},
): MetaClient {
  const ydoc = new Y.Doc();
  const metaFiles = ydoc.getMap<Y.Map<unknown>>("files");
  const metaEvents = ydoc.getArray<Y.Map<unknown>>("events");
  const metaServerState = ydoc.getMap<unknown>("serverState");
  const websocket = new HocuspocusProviderWebsocket({
    url,
    maxAttempts: 0,
    messageReconnectTimeout: 30_000,
  });
  const syncedResolvers: Array<() => void> = [];
  const statelessMessages: Array<Record<string, unknown>> = [];
  const statelessWaiters: Array<{
    predicate: (message: Record<string, unknown>) => boolean;
    resolve: (message: Record<string, unknown>) => void;
  }> = [];

  const provider = new HocuspocusProvider({
    websocketProvider: websocket,
    name: META_DOCUMENT_NAME,
    document: ydoc,
    token: options.token !== undefined ? options.token : TEST_TOKEN,
    onAuthenticationFailed: () => {
      options.onAuthFailed?.();
    },
    onSynced: () => {
      const resolvers = syncedResolvers.splice(0);
      for (const resolve of resolvers) {
        resolve();
      }
    },
    onStateless: ({ payload }) => {
      let message: Record<string, unknown>;
      try {
        message = JSON.parse(payload) as Record<string, unknown>;
      } catch {
        return;
      }

      statelessMessages.push(message);
      for (const waiter of [...statelessWaiters]) {
        if (!waiter.predicate(message)) {
          continue;
        }

        const index = statelessWaiters.indexOf(waiter);
        if (index !== -1) {
          statelessWaiters.splice(index, 1);
        }
        waiter.resolve(message);
      }
    },
  });

  provider.attach();

  return {
    provider,
    ydoc,
    metaFiles,
    metaEvents,
    metaServerState,
    statelessMessages,
    waitForSync: createSyncWaiter(provider, syncedResolvers),
    waitForStateless(predicate, timeout = TIMEOUT) {
      const existing = statelessMessages.find(predicate);
      if (existing) {
        return Promise.resolve(existing);
      }

      return new Promise<Record<string, unknown>>((resolve, reject) => {
        const timer = setTimeout(() => {
          const index = statelessWaiters.findIndex((waiter) => {
            return waiter.resolve === onResolve;
          });
          if (index !== -1) {
            statelessWaiters.splice(index, 1);
          }
          reject(new Error(`Stateless timeout after ${timeout}ms`));
        }, timeout);

        const onResolve = (message: Record<string, unknown>) => {
          clearTimeout(timer);
          resolve(message);
        };

        statelessWaiters.push({
          predicate,
          resolve: onResolve,
        });
      });
    },
    sendStateless(message) {
      provider.sendStateless(JSON.stringify(message));
    },
    destroy() {
      provider.destroy();
      websocket.destroy();
    },
  };
}

interface ContentClient {
  ydoc: Y.Doc;
  filesMap: Y.Map<Y.Text | Y.Array<Uint8Array>>;
  waitForSync: (timeout?: number) => Promise<void>;
  destroy: () => void;
}

function createContentClient(url: string): ContentClient {
  const ydoc = new Y.Doc();
  const filesMap = ydoc.getMap<Y.Text | Y.Array<Uint8Array>>("files");
  const websocket = new HocuspocusProviderWebsocket({
    url,
    maxAttempts: 0,
    messageReconnectTimeout: 30_000,
  });
  const syncedResolvers: Array<() => void> = [];

  const provider = new HocuspocusProvider({
    websocketProvider: websocket,
    name: CONTENT_DOCUMENT_NAME,
    document: ydoc,
    token: TEST_TOKEN,
    onSynced: () => {
      const resolvers = syncedResolvers.splice(0);
      for (const resolve of resolvers) {
        resolve();
      }
    },
  });

  provider.attach();

  return {
    ydoc,
    filesMap,
    waitForSync: createSyncWaiter(provider, syncedResolvers),
    destroy() {
      provider.destroy();
      websocket.destroy();
    },
  };
}

function getAutoVacuumMode(): number {
  const sqlite = new BunSQLite(join(dataDir, "sync.db"), { readonly: true });
  try {
    const row = sqlite.query("PRAGMA auto_vacuum").get() as {
      auto_vacuum?: number;
    } | null;
    return row?.auto_vacuum ?? 0;
  } finally {
    sqlite.close();
  }
}

function metaFile(
  client: MetaClient,
  fileId: string,
): Y.Map<unknown> | undefined {
  return client.metaFiles.get(fileId);
}

function metaEventIds(client: MetaClient): number[] {
  return client.metaEvents
    .toArray()
    .map((event) => event.get("eventId") as number);
}

let server: Awaited<ReturnType<typeof createSyncServer>>;
let url: string;
let httpUrl: string;
let dataDir: string;
const activeMetaClients: MetaClient[] = [];
const activeContentClients: ContentClient[] = [];

function metaClient(
  options: Parameters<typeof createMetaClient>[1] = {},
): MetaClient {
  const client = createMetaClient(url, options);
  activeMetaClients.push(client);
  return client;
}

function contentClient(): ContentClient {
  const client = createContentClient(url);
  activeContentClients.push(client);
  return client;
}

function destroyAllClients(): void {
  for (const client of activeMetaClients.splice(0)) {
    try {
      client.destroy();
    } catch {}
  }

  for (const client of activeContentClients.splice(0)) {
    try {
      client.destroy();
    } catch {}
  }
}

beforeEach(async () => {
  const started = await startTestServer();
  server = started.server;
  url = started.url;
  httpUrl = started.httpUrl;
  dataDir = started.dataDir;
});

afterEach(async () => {
  destroyAllClients();
  if (server) {
    await server.destroy();
    await Bun.sleep(100);
  }
  if (dataDir) {
    await $`rm -rf ${dataDir}`.quiet();
  }
});

describe("Auth", () => {
  test(
    "rejects metadata connections with an invalid token",
    async () => {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error("Auth failure not received")),
          TIMEOUT,
        );

        metaClient({
          token: "wrong-token",
          onAuthFailed: () => {
            clearTimeout(timer);
            resolve();
          },
        });
      });
    },
    TIMEOUT,
  );
});

describe("HTTP", () => {
  test("GET /health returns status only", async () => {
    const response = await fetch(`${httpUrl}/health`);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(response.headers.get("x-content-type-options")).toBe("nosniff");

    const body = (await response.json()) as Record<string, unknown>;
    expect(body.status).toBe("ok");
    expect(body.connections).toBeUndefined();
    expect(body.documents).toBeUndefined();
  });
});

describe("vault-content", () => {
  test("enables SQLite auto-vacuum so freed pages can shrink the database", () => {
    expect(getAutoVacuumMode()).toBe(1);
  });

  test(
    "remains writable while vault-meta stays read-only",
    async () => {
      const first = contentClient();
      const second = contentClient();
      await Promise.all([first.waitForSync(), second.waitForSync()]);

      first.ydoc.transact(() => {
        const text = new Y.Text();
        text.insert(0, "content works");
        first.filesMap.set("note.md", text);
      }, "local");

      await waitFor(
        () => second.filesMap.get("note.md")?.toString() === "content works",
      );
      expect(second.filesMap.get("note.md")?.toString()).toBe("content works");
    },
    TIMEOUT,
  );

  test(
    "removes deleted binary content from vault-content",
    async () => {
      const meta = metaClient();
      const uploader = contentClient();
      const observer = contentClient();
      await Promise.all([
        meta.waitForSync(),
        uploader.waitForSync(),
        observer.waitForSync(),
      ]);

      meta.sendStateless({
        type: "file.create",
        fileId: "file-binary",
        path: "photo.bin",
        kind: "binary",
        clientId: "client-a",
        clientOpId: "op-create-binary",
        timestamp: 100,
      });

      await waitFor(
        () => metaFile(meta, "file-binary")?.get("path") === "photo.bin",
      );

      const bytes = new Uint8Array(256 * 1024);
      bytes.fill(7);
      uploader.ydoc.transact(() => {
        const binary = new Y.Array<Uint8Array>();
        binary.push([bytes]);
        uploader.filesMap.set("file-binary", binary);
      }, "local");

      await waitFor(
        () => observer.filesMap.get("file-binary") instanceof Y.Array,
      );

      meta.sendStateless({
        type: "file.delete",
        fileId: "file-binary",
        path: "photo.bin",
        clientId: "client-a",
        clientOpId: "op-delete-binary",
        timestamp: 101,
      });

      await waitFor(() => {
        return (
          metaFile(meta, "file-binary")?.get("deleted") === true &&
          !uploader.filesMap.has("file-binary") &&
          !observer.filesMap.has("file-binary")
        );
      });

      const deleteEvent = meta.metaEvents
        .toArray()
        .find((event) => event.get("clientOpId") === "op-delete-binary");
      expect(typeof deleteEvent?.get("contentFingerprint")).toBe("string");
    },
    TIMEOUT,
  );

  test(
    "removes descendant content entries when a directory is deleted",
    async () => {
      const meta = metaClient();
      const content = contentClient();
      await Promise.all([meta.waitForSync(), content.waitForSync()]);

      meta.sendStateless({
        type: "file.create",
        fileId: "dir-1",
        path: "folder",
        kind: "directory",
        clientId: "client-a",
        clientOpId: "op-create-dir",
        timestamp: 100,
      });
      meta.sendStateless({
        type: "file.create",
        fileId: "file-1",
        path: "folder/a.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-create-a",
        timestamp: 101,
      });
      meta.sendStateless({
        type: "file.create",
        fileId: "file-2",
        path: "folder/b.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-create-b",
        timestamp: 102,
      });

      await waitFor(() => {
        return (
          metaFile(meta, "file-1")?.get("path") === "folder/a.md" &&
          metaFile(meta, "file-2")?.get("path") === "folder/b.md"
        );
      });

      content.ydoc.transact(() => {
        const first = new Y.Text();
        first.insert(0, "A");
        content.filesMap.set("file-1", first);
        const second = new Y.Text();
        second.insert(0, "B");
        content.filesMap.set("file-2", second);
      }, "local");

      await waitFor(() => {
        return (
          content.filesMap.get("file-1") instanceof Y.Text &&
          content.filesMap.get("file-2") instanceof Y.Text
        );
      });

      meta.sendStateless({
        type: "file.delete",
        fileId: "dir-1",
        path: "folder",
        clientId: "client-a",
        clientOpId: "op-delete-dir",
        timestamp: 103,
      });

      await waitFor(() => {
        return (
          metaFile(meta, "dir-1")?.get("deleted") === true &&
          metaFile(meta, "file-1")?.get("deleted") === true &&
          metaFile(meta, "file-2")?.get("deleted") === true &&
          !content.filesMap.has("file-1") &&
          !content.filesMap.has("file-2")
        );
      });
    },
    TIMEOUT,
  );

  test(
    "deduplicates file.delete for a file already deleted by parent directory cascade",
    async () => {
      const meta = metaClient();
      await meta.waitForSync();

      // Create a directory with one file inside
      meta.sendStateless({
        type: "file.create",
        fileId: "dir-1",
        path: "folder",
        kind: "directory",
        clientId: "client-a",
        clientOpId: "op-create-dir",
        timestamp: 100,
      });
      meta.sendStateless({
        type: "file.create",
        fileId: "file-1",
        path: "folder/note.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-create-file",
        timestamp: 101,
      });

      await waitFor(() => {
        return metaFile(meta, "file-1")?.get("path") === "folder/note.md";
      });

      // Delete the directory first (this cascades and marks file-1 as deleted)
      meta.sendStateless({
        type: "file.delete",
        fileId: "dir-1",
        path: "folder",
        clientId: "client-a",
        clientOpId: "op-delete-dir",
        timestamp: 102,
      });

      // Wait for cascade to mark the child as deleted
      await waitFor(() => {
        return (
          metaFile(meta, "dir-1")?.get("deleted") === true &&
          metaFile(meta, "file-1")?.get("deleted") === true
        );
      });

      // Now send an individual file.delete for the same file (simulates plugin
      // sending per-file delete ops alongside the directory delete)
      meta.sendStateless({
        type: "file.delete",
        fileId: "file-1",
        path: "folder/note.md",
        clientId: "client-a",
        clientOpId: "op-delete-file",
        timestamp: 103,
      });

      // Should receive a commit (deduplicated), not a reject
      const commit = await meta.waitForStateless(
        (msg) =>
          msg.type === "metadata.commit" &&
          msg.fileId === "file-1" &&
          msg.clientOpId === "op-delete-file",
      );

      expect(commit.type).toBe("metadata.commit");
      expect(commit.deduplicated).toBe(true);
      expect(commit.fileId).toBe("file-1");
    },
    TIMEOUT,
  );
});

describe("vault-meta", () => {
  test(
    "initializes files, events, and serverState on first load",
    async () => {
      const client = metaClient();
      await client.waitForSync();

      await waitFor(() => {
        return (
          client.metaServerState.get("lastEventId") === 0 &&
          client.metaServerState.get("serverEpoch") === 1
        );
      });

      expect(client.metaFiles.size).toBe(0);
      expect(client.metaEvents.length).toBe(0);
      expect(client.metaServerState.get("lastEventId")).toBe(0);
      expect(client.metaServerState.get("serverEpoch")).toBe(1);
    },
    TIMEOUT,
  );

  test(
    "create, rename, and delete mutate vault-meta and increment lastEventId",
    async () => {
      const first = metaClient();
      const second = metaClient();
      await Promise.all([first.waitForSync(), second.waitForSync()]);

      first.sendStateless({
        type: "file.create",
        fileId: "file-1",
        path: "note.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-1",
        timestamp: 100,
      });

      await waitFor(
        () => metaFile(second, "file-1")?.get("path") === "note.md",
      );

      first.sendStateless({
        type: "file.rename",
        fileId: "file-1",
        oldPath: "note.md",
        newPath: "renamed.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-2",
        timestamp: 101,
      });

      await waitFor(
        () => metaFile(second, "file-1")?.get("path") === "renamed.md",
      );

      first.sendStateless({
        type: "file.delete",
        fileId: "file-1",
        path: "renamed.md",
        clientId: "client-a",
        clientOpId: "op-3",
        timestamp: 102,
      });

      await waitFor(() => {
        return (
          metaFile(second, "file-1")?.get("deleted") === true &&
          second.metaEvents.length === 3 &&
          second.metaServerState.get("lastEventId") === 3
        );
      });

      const file = metaFile(second, "file-1");
      expect(file?.get("path")).toBe("renamed.md");
      expect(file?.get("deleted")).toBe(true);
      expect(file?.get("createdAt")).toBe(100);
      expect(file?.get("updatedAt")).toBe(102);

      const events = second.metaEvents.toArray();
      expect(events.map((event) => event.get("type"))).toEqual([
        "file.create",
        "file.rename",
        "file.delete",
      ]);
      expect(metaEventIds(second)).toEqual([1, 2, 3]);
      expect(events[1]?.get("oldPath")).toBe("note.md");
      expect(events[1]?.get("newPath")).toBe("renamed.md");
      expect(events[2]?.get("path")).toBe("renamed.md");
    },
    TIMEOUT,
  );

  test(
    "rejects one of two concurrent creates for the same path and preserves one canonical event",
    async () => {
      const first = metaClient();
      const second = metaClient();
      await Promise.all([first.waitForSync(), second.waitForSync()]);

      first.sendStateless({
        type: "file.create",
        fileId: "file-a",
        path: "shared.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-a",
        timestamp: 300,
      });
      second.sendStateless({
        type: "file.create",
        fileId: "file-b",
        path: "shared.md",
        kind: "text",
        clientId: "client-b",
        clientOpId: "op-b",
        timestamp: 301,
      });

      await Promise.all([
        first.waitForStateless((message) => message.type === "metadata.commit"),
        second.waitForStateless(
          (message) => message.type === "metadata.commit",
        ),
        waitFor(() => {
          return (
            first.statelessMessages.some(
              (message) => message.type === "metadata.reject",
            ) ||
            second.statelessMessages.some(
              (message) => message.type === "metadata.reject",
            )
          );
        }),
      ]);

      await waitFor(
        () => first.metaEvents.length === 1 && second.metaEvents.length === 1,
      );

      const allMessages = [
        ...first.statelessMessages,
        ...second.statelessMessages,
      ];
      expect(
        allMessages.filter((message) => message.type === "metadata.reject"),
      ).toHaveLength(1);
      const commit = allMessages.find(
        (message) => message.type === "metadata.commit",
      );
      const reject = allMessages.find(
        (message) => message.type === "metadata.reject",
      );

      expect(commit).toMatchObject({
        type: "metadata.commit",
        requestType: "file.create",
        metaEventId: 1,
        path: "shared.md",
      });
      expect(reject).toMatchObject({
        type: "metadata.reject",
        requestType: "file.create",
      });
      const committedFileId = commit?.fileId;
      const rejectedFileId = reject?.fileId;
      expect(typeof committedFileId).toBe("string");
      expect(typeof rejectedFileId).toBe("string");
      expect(committedFileId).not.toBe(rejectedFileId);
      expect(first.metaFiles.has(committedFileId as string)).toBe(true);
      expect(first.metaFiles.has(rejectedFileId as string)).toBe(false);
      expect(metaEventIds(first)).toEqual([1]);
      expect(first.metaServerState.get("lastEventId")).toBe(1);
    },
    TIMEOUT,
  );

  test(
    "rejects a rename after a conflicting delete and keeps metadata canonical",
    async () => {
      const creator = metaClient();
      const deleter = metaClient();
      const renamer = metaClient();
      await Promise.all([
        creator.waitForSync(),
        deleter.waitForSync(),
        renamer.waitForSync(),
      ]);

      creator.sendStateless({
        type: "file.create",
        fileId: "file-1",
        path: "conflict.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-create",
        timestamp: 400,
      });
      await waitFor(
        () => metaFile(renamer, "file-1")?.get("path") === "conflict.md",
      );

      deleter.sendStateless({
        type: "file.delete",
        fileId: "file-1",
        path: "conflict.md",
        clientId: "client-b",
        clientOpId: "op-delete",
        timestamp: 401,
      });

      await deleter.waitForStateless((message) => {
        return (
          message.type === "metadata.commit" &&
          message.clientOpId === "op-delete"
        );
      });

      renamer.sendStateless({
        type: "file.rename",
        fileId: "file-1",
        oldPath: "conflict.md",
        newPath: "renamed.md",
        kind: "text",
        clientId: "client-c",
        clientOpId: "op-rename",
        timestamp: 402,
      });

      const reject = await renamer.waitForStateless((message) => {
        return (
          message.type === "metadata.reject" &&
          message.clientOpId === "op-rename"
        );
      });

      expect(reject).toMatchObject({
        type: "metadata.reject",
        requestType: "file.rename",
        fileId: "file-1",
        currentPath: "conflict.md",
      });
      expect(metaFile(creator, "file-1")?.get("deleted")).toBe(true);
      expect(metaFile(creator, "file-1")?.get("path")).toBe("conflict.md");
      expect(metaEventIds(creator)).toEqual([1, 2]);
    },
    TIMEOUT,
  );

  test(
    "deduplicates offline rename replay by clientId and clientOpId",
    async () => {
      const client = metaClient();
      await client.waitForSync();

      client.sendStateless({
        type: "file.create",
        fileId: "file-1",
        path: "draft.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-create",
        timestamp: 500,
      });
      await waitFor(
        () => metaFile(client, "file-1")?.get("path") === "draft.md",
      );

      const renameRequest = {
        type: "file.rename",
        fileId: "file-1",
        oldPath: "draft.md",
        newPath: "renamed.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-rename",
        timestamp: 501,
      };
      client.sendStateless(renameRequest);

      const firstCommit = await client.waitForStateless((message) => {
        return (
          message.type === "metadata.commit" &&
          message.clientOpId === "op-rename"
        );
      });

      client.sendStateless(renameRequest);
      const replayCommit = await client.waitForStateless((message) => {
        return (
          message.type === "metadata.commit" &&
          message.clientOpId === "op-rename" &&
          message.deduplicated === true
        );
      });

      expect(firstCommit).toMatchObject({
        type: "metadata.commit",
        requestType: "file.rename",
        metaEventId: 2,
        newPath: "renamed.md",
      });
      expect(replayCommit).toMatchObject({
        type: "metadata.commit",
        requestType: "file.rename",
        metaEventId: 2,
        deduplicated: true,
      });
      expect(metaEventIds(client)).toEqual([1, 2]);
      expect(client.metaServerState.get("lastEventId")).toBe(2);
    },
    TIMEOUT,
  );

  test(
    "deduplicates duplicate stateless replay storms without appending extra metadata events",
    async () => {
      const client = metaClient();
      await client.waitForSync();

      client.sendStateless({
        type: "file.create",
        fileId: "file-1",
        path: "storm.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-create",
        timestamp: 700,
      });
      await waitFor(
        () => metaFile(client, "file-1")?.get("path") === "storm.md",
      );

      const renameRequest = {
        type: "file.rename",
        fileId: "file-1",
        oldPath: "storm.md",
        newPath: "storm-renamed.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-rename-storm",
        timestamp: 701,
      };

      for (let attempt = 0; attempt < 10; attempt += 1) {
        client.sendStateless(renameRequest);
      }

      await waitFor(() => {
        return (
          client.statelessMessages.filter((message) => {
            return (
              message.type === "metadata.commit" &&
              message.clientOpId === "op-rename-storm"
            );
          }).length === 10
        );
      });

      const renameCommits = client.statelessMessages.filter((message) => {
        return (
          message.type === "metadata.commit" &&
          message.clientOpId === "op-rename-storm"
        );
      });

      expect(renameCommits).toHaveLength(10);
      expect(renameCommits[0]).toMatchObject({
        type: "metadata.commit",
        requestType: "file.rename",
        metaEventId: 2,
        deduplicated: false,
      });
      expect(
        renameCommits.slice(1).every((message) => {
          return (
            message.requestType === "file.rename" &&
            message.metaEventId === 2 &&
            message.deduplicated === true
          );
        }),
      ).toBe(true);
      expect(metaEventIds(client)).toEqual([1, 2]);
      expect(client.metaServerState.get("lastEventId")).toBe(2);
      expect(metaFile(client, "file-1")?.get("path")).toBe("storm-renamed.md");
    },
    TIMEOUT,
  );

  test(
    "blocks direct client metadata edits",
    async () => {
      const first = metaClient();
      const second = metaClient();
      await Promise.all([first.waitForSync(), second.waitForSync()]);

      first.ydoc.transact(() => {
        const rogueMetadata = new Y.Map<unknown>();
        rogueMetadata.set("path", "rogue.md");
        rogueMetadata.set("deleted", false);
        first.metaFiles.set("rogue-file", rogueMetadata);
      }, "local");

      await waitFor(() => first.provider.hasUnsyncedChanges);
      await Bun.sleep(300);

      expect(second.metaFiles.has("rogue-file")).toBe(false);

      const fresh = metaClient();
      await fresh.waitForSync();
      await Bun.sleep(100);

      expect(fresh.metaFiles.has("rogue-file")).toBe(false);
    },
    TIMEOUT,
  );

  test(
    "survives restart with monotonic event ids intact",
    async () => {
      const first = metaClient();
      await first.waitForSync();

      first.sendStateless({
        type: "file.create",
        fileId: "file-1",
        path: "persisted.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-1",
        timestamp: 200,
      });
      first.sendStateless({
        type: "file.rename",
        fileId: "file-1",
        oldPath: "persisted.md",
        newPath: "persisted-renamed.md",
        kind: "text",
        clientId: "client-a",
        clientOpId: "op-2",
        timestamp: 201,
      });

      await waitFor(() => {
        return (
          metaFile(first, "file-1")?.get("path") === "persisted-renamed.md" &&
          first.metaServerState.get("lastEventId") === 2
        );
      });

      destroyAllClients();
      await server.destroy();
      await Bun.sleep(100);

      const restarted = await startTestServer(dataDir);
      server = restarted.server;
      url = restarted.url;
      httpUrl = restarted.httpUrl;
      const observer = metaClient();
      await observer.waitForSync();

      await waitFor(() => {
        return (
          metaFile(observer, "file-1")?.get("path") ===
            "persisted-renamed.md" &&
          observer.metaServerState.get("lastEventId") === 2 &&
          observer.metaEvents.length === 2
        );
      });

      expect(metaEventIds(observer)).toEqual([1, 2]);
      expect(metaFile(observer, "file-1")?.get("deleted")).toBe(false);
    },
    TIMEOUT,
  );
});

describe("Path validation", () => {
  test(
    "accepts create request for a non-markdown text path",
    async () => {
      const client = metaClient();
      await client.waitForSync();

      client.sendStateless({
        type: "file.create",
        clientId: "client-1",
        clientOpId: "op-1",
        fileId: crypto.randomUUID(),
        path: "notes/note.txt",
        kind: "text",
        timestamp: Date.now(),
      });

      const commit = await client.waitForStateless(
        (msg) => msg.type === "metadata.commit" && msg.clientOpId === "op-1",
      );

      expect(commit).toMatchObject({
        type: "metadata.commit",
        clientOpId: "op-1",
        path: "notes/note.txt",
        kind: "text",
      });
      expect(client.metaEvents.length).toBe(1);
    },
    TIMEOUT,
  );

  test(
    "rejects create request with a path traversal segment",
    async () => {
      const client = metaClient();
      await client.waitForSync();

      client.sendStateless({
        type: "file.create",
        clientId: "client-1",
        clientOpId: "op-2",
        fileId: crypto.randomUUID(),
        path: "../outside/vault.md",
        kind: "text",
        timestamp: Date.now(),
      });

      const reject = await client.waitForStateless(
        (msg) => msg.type === "metadata.reject" && msg.clientOpId === "op-2",
      );

      expect(reject.type).toBe("metadata.reject");
      expect(client.metaEvents.length).toBe(0);
    },
    TIMEOUT,
  );

  test(
    "rejects create request for an ignored path",
    async () => {
      const client = metaClient();
      await client.waitForSync();

      client.sendStateless({
        type: "file.create",
        clientId: "client-1",
        clientOpId: "op-ignored-create",
        fileId: crypto.randomUUID(),
        path: ".obsidian/workspace.json",
        kind: "text",
        timestamp: Date.now(),
      });

      const reject = await client.waitForStateless(
        (msg) =>
          msg.type === "metadata.reject" &&
          msg.clientOpId === "op-ignored-create",
      );

      expect(reject).toMatchObject({
        type: "metadata.reject",
        requestType: "file.create",
        reason: 'Path ".obsidian/workspace.json" is ignored',
      });
      expect(client.metaEvents.length).toBe(0);
    },
    TIMEOUT,
  );

  test(
    "accepts rename request for a non-markdown path",
    async () => {
      const fileId = crypto.randomUUID();
      const creator = metaClient();
      await creator.waitForSync();

      creator.sendStateless({
        type: "file.create",
        clientId: "client-1",
        clientOpId: "op-create",
        fileId,
        path: "note.md",
        kind: "text",
        timestamp: Date.now(),
      });

      await creator.waitForStateless(
        (msg) =>
          msg.type === "metadata.commit" && msg.clientOpId === "op-create",
      );

      const client = metaClient();
      await client.waitForSync();

      client.sendStateless({
        type: "file.rename",
        clientId: "client-1",
        clientOpId: "op-rename",
        fileId,
        oldPath: "note.md",
        newPath: "note.txt",
        kind: "text",
        timestamp: Date.now(),
      });

      const commit = await client.waitForStateless(
        (msg) =>
          msg.type === "metadata.commit" && msg.clientOpId === "op-rename",
      );

      expect(commit).toMatchObject({
        type: "metadata.commit",
        clientOpId: "op-rename",
        newPath: "note.txt",
        kind: "text",
      });
      expect(client.metaEvents.length).toBe(2);
    },
    TIMEOUT,
  );

  test(
    "rejects rename request into an ignored path",
    async () => {
      const fileId = crypto.randomUUID();
      const creator = metaClient();
      await creator.waitForSync();

      creator.sendStateless({
        type: "file.create",
        clientId: "client-1",
        clientOpId: "op-create-rename-ignored",
        fileId,
        path: "note.md",
        kind: "text",
        timestamp: Date.now(),
      });

      await creator.waitForStateless(
        (msg) =>
          msg.type === "metadata.commit" &&
          msg.clientOpId === "op-create-rename-ignored",
      );

      const client = metaClient();
      await client.waitForSync();

      client.sendStateless({
        type: "file.rename",
        clientId: "client-1",
        clientOpId: "op-rename-ignored",
        fileId,
        oldPath: "note.md",
        newPath: ".obsidian/workspace.json",
        kind: "text",
        timestamp: Date.now(),
      });

      const reject = await client.waitForStateless(
        (msg) =>
          msg.type === "metadata.reject" &&
          msg.clientOpId === "op-rename-ignored",
      );

      expect(reject).toMatchObject({
        type: "metadata.reject",
        requestType: "file.rename",
        reason: 'Path ".obsidian/workspace.json" is ignored',
      });
      expect(client.metaEvents.length).toBe(1);
    },
    TIMEOUT,
  );
});

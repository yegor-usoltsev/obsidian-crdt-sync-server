/**
 * Main sync server: HTTP health endpoint, WebSocket control channel,
 * Hocuspocus text doc channels, and blob HTTP endpoints.
 */

import type { Database } from "bun:sqlite";
import type { Hocuspocus } from "@hocuspocus/server";
import type { Server as BunServer, ServerWebSocket } from "bun";
import type { BlobStore } from "../blob-store/blob-store";
import type { HistoryStore } from "../history/history-store";
import type { MetadataRegistry } from "../metadata-registry/registry";
import type { SettingsStore } from "../settings-store/settings-store";
import { log } from "../shared/log";
import { isRateLimited, recordAuthFailure, verifyToken } from "./auth";
import { BunWsAdapter } from "./bun-ws-shim";
import {
  type ControlResponse,
  type MetadataCommitResponse,
  parseControlMessage,
} from "./messages";

/** Payload size limits. */
export const PAYLOAD_LIMITS = {
  metadata: 256 * 1024, // 256 KiB
  content: 200 * 1024 * 1024, // 200 MiB
} as const;

export interface SyncServerConfig {
  port: number;
  authToken: string;
  db: Database;
  registry: MetadataRegistry;
  historyStore: HistoryStore;
  blobStore: BlobStore;
  settingsStore: SettingsStore;
  textDocService: Hocuspocus;
}

export interface SyncServer {
  start(): Promise<void>;
  stop(): Promise<void>;
  readonly port: number;
  /** Broadcast a message to all authenticated WebSocket clients. */
  broadcast(msg: ControlResponse): void;
}

interface WsData {
  authenticated: boolean;
  clientId?: string;
  subscribedRevision?: number;
  docFileId?: string;
  docAdapter?: BunWsAdapter;
}

/**
 * Create and configure the sync server.
 */
export function createSyncServer(config: SyncServerConfig): SyncServer {
  let server: BunServer<WsData> | null = null;
  const wsClients = new Set<ServerWebSocket<WsData>>();

  function broadcast(msg: ControlResponse, exclude?: ServerWebSocket<WsData>) {
    const text = JSON.stringify(msg);
    for (const ws of wsClients) {
      if (ws !== exclude && ws.data.authenticated) {
        ws.send(text);
      }
    }
  }

  function send(ws: ServerWebSocket<WsData>, msg: ControlResponse) {
    ws.send(JSON.stringify(msg));
  }

  return {
    get port() {
      return server?.port ?? config.port;
    },

    broadcast(msg: ControlResponse) {
      broadcast(msg);
    },

    async start() {
      server = Bun.serve<WsData>({
        port: config.port,

        async fetch(req, srv) {
          const url = new URL(req.url);

          // Health endpoint — no auth required, no sync state leaked
          if (url.pathname === "/health" && req.method === "GET") {
            return new Response(JSON.stringify({ status: "ok" }), {
              headers: { "Content-Type": "application/json" },
            });
          }

          // WebSocket upgrade for control channel
          if (url.pathname === "/ws") {
            const source =
              req.headers.get("x-forwarded-for") ??
              req.headers.get("x-real-ip") ??
              "unknown";

            if (isRateLimited(source)) {
              return new Response("Too Many Requests", { status: 429 });
            }

            // Extract token from query param or header
            const tokenParam = url.searchParams.get("token");
            const authHeader = req.headers.get("authorization");
            const token =
              tokenParam ??
              (authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null);

            if (!token || !verifyToken(token, config.authToken)) {
              if (token) recordAuthFailure(source);
              return new Response("Unauthorized", { status: 401 });
            }

            const clientId =
              req.headers.get("x-client-id") ??
              url.searchParams.get("clientId") ??
              crypto.randomUUID();

            const upgraded = srv.upgrade(req, {
              data: { authenticated: true, clientId },
            });
            if (upgraded) return undefined as unknown as Response;
            return new Response("WebSocket upgrade failed", { status: 500 });
          }

          // WebSocket upgrade for text-doc sync (/docs/:fileId)
          if (url.pathname.startsWith("/docs/")) {
            const fileId = decodeURIComponent(
              url.pathname.slice("/docs/".length),
            );
            if (!fileId) {
              return new Response("Bad Request: missing file ID", {
                status: 400,
              });
            }

            const docSource =
              req.headers.get("x-forwarded-for") ??
              req.headers.get("x-real-ip") ??
              "unknown";

            if (isRateLimited(docSource)) {
              return new Response("Too Many Requests", { status: 429 });
            }

            const docTokenParam = url.searchParams.get("token");
            const docAuthHeader = req.headers.get("authorization");
            const docToken =
              docTokenParam ??
              (docAuthHeader?.startsWith("Bearer ")
                ? docAuthHeader.slice(7)
                : null);

            if (!docToken || !verifyToken(docToken, config.authToken)) {
              if (docToken) recordAuthFailure(docSource);
              return new Response("Unauthorized", { status: 401 });
            }

            const docUpgraded = srv.upgrade(req, {
              data: { authenticated: true, docFileId: fileId },
            });
            if (docUpgraded) return undefined as unknown as Response;
            return new Response("WebSocket upgrade failed", { status: 500 });
          }

          // Auth check for HTTP endpoints
          const source =
            req.headers.get("x-forwarded-for") ??
            req.headers.get("x-real-ip") ??
            "unknown";

          if (isRateLimited(source)) {
            return new Response("Too Many Requests", { status: 429 });
          }

          const authHeader = req.headers.get("authorization");
          const token = authHeader?.startsWith("Bearer ")
            ? authHeader.slice(7)
            : null;

          if (!token || !verifyToken(token, config.authToken)) {
            if (token) recordAuthFailure(source);
            return new Response("Unauthorized", { status: 401 });
          }

          // Blob endpoints
          if (url.pathname.startsWith("/blobs/")) {
            return handleBlobRequest(req, url, config, broadcast);
          }

          // Settings endpoints
          if (url.pathname.startsWith("/settings/")) {
            return handleSettingsRequest(req, url, config, broadcast);
          }

          return new Response("Not Found", { status: 404 });
        },

        websocket: {
          open(ws) {
            if (ws.data.docFileId) {
              // Route to Hocuspocus for text-doc sync via BunWsAdapter shim.
              try {
                const adapter = new BunWsAdapter(ws);
                ws.data.docAdapter = adapter;
                // Hocuspocus expects an IncomingMessage-like object with
                // headers and url properties. Since Bun doesn't provide
                // this via the WebSocket upgrade, we supply a minimal shim.
                const mockRequest = {
                  headers: {},
                  url: `/docs/${ws.data.docFileId}`,
                } as unknown as import("http").IncomingMessage;
                config.textDocService.handleConnection(
                  adapter as unknown as import("ws").WebSocket,
                  mockRequest,
                  { documentName: ws.data.docFileId },
                );
              } catch (err) {
                log("error", "Hocuspocus handleConnection failed", {
                  error: err instanceof Error ? err.message : String(err),
                  fileId: ws.data.docFileId,
                });
                ws.close();
              }
              return;
            }
            wsClients.add(ws);
            log("info", "WebSocket client connected");
          },

          message(ws, message) {
            // Forward doc messages to Hocuspocus via adapter
            if (ws.data.docAdapter) {
              ws.data.docAdapter._emitMessage(
                typeof message === "string"
                  ? message
                  : (message as unknown as ArrayBuffer),
              );
              return;
            }

            const parsed = parseControlMessage(String(message));

            if ("error" in parsed) {
              send(ws, { action: "error", message: parsed.error });
              return;
            }

            switch (parsed.action) {
              case "ping":
                send(ws, { action: "pong" });
                break;

              case "metadata.intent": {
                const result = config.registry.processIntent(parsed.payload);

                if ("reason" in result) {
                  send(ws, {
                    action: "metadata.reject",
                    payload: result,
                  });
                } else {
                  // Check if this is a replay (same revision already exists)
                  const state = config.registry.getState();
                  const isReplay = result.revision < state.revision;

                  const commitPayload = {
                    ...result,
                  };

                  // Always send commit to the requesting client
                  send(ws, {
                    action: "metadata.commit",
                    payload: commitPayload,
                  });

                  // Broadcast new commits to other clients (not replays)
                  if (!isReplay) {
                    broadcast(
                      {
                        action: "metadata.commit",
                        payload: commitPayload,
                      },
                      ws,
                    );
                  }
                }
                break;
              }

              case "metadata.subscribe": {
                ws.data.subscribedRevision = parsed.sinceRevision ?? 0;
                // Send any commits since the requested revision
                const sinceRev = parsed.sinceRevision ?? 0;
                const entries = config.historyStore.getHistorySince(sinceRev);
                for (const entry of entries) {
                  send(ws, {
                    action: "metadata.commit",
                    payload: {
                      operationId: entry.operationId,
                      fileId: entry.fileId,
                      path: entry.path,
                      kind: entry.kind,
                      deleted: entry.operationType === "delete",
                      contentAnchor: entry.contentAnchor,
                      revision: entry.revision,
                      epoch: entry.epoch,
                      operationType: entry.operationType,
                      contentDigest: entry.contentDigest ?? undefined,
                      contentSize: entry.contentSize ?? undefined,
                    },
                  });
                }

                // Send replay-complete signal
                const currentState = config.registry.getState();
                ws.send(
                  JSON.stringify({
                    action: "metadata.replay-complete",
                    sinceRevision: sinceRev,
                    currentRevision: currentState.revision,
                    ...(parsed.requestId !== undefined && {
                      requestId: parsed.requestId,
                    }),
                  }),
                );
                break;
              }

              case "history.list": {
                const entries = config.historyStore.getFileHistory(
                  parsed.fileId,
                );
                ws.send(
                  JSON.stringify({
                    action: "history.list",
                    payload: entries,
                    ...(parsed.requestId !== undefined && {
                      requestId: parsed.requestId,
                    }),
                  }),
                );
                break;
              }

              case "history.restore": {
                const restored = config.historyStore.restore(
                  parsed.fileId,
                  parsed.historyEntryId,
                  ws.data.clientId ?? "unknown",
                );
                if (restored) {
                  ws.send(
                    JSON.stringify({
                      action: "history.restored",
                      payload: restored,
                      ...(parsed.requestId !== undefined && {
                        requestId: parsed.requestId,
                      }),
                    }),
                  );

                  // Broadcast restore commit to ALL connected clients (including requester).
                  // The requester also needs the commit to materialize non-text restores.
                  broadcast({
                    action: "metadata.commit",
                    payload: {
                      operationId: restored.operationId,
                      fileId: restored.fileId,
                      path: restored.path,
                      kind: restored.kind,
                      deleted: false,
                      contentAnchor: restored.contentAnchor,
                      revision: restored.revision,
                      epoch: restored.epoch,
                      operationType: "restore",
                      contentDigest: restored.contentDigest ?? undefined,
                      contentSize: restored.contentSize ?? undefined,
                    },
                  });
                } else {
                  send(ws, {
                    action: "error",
                    message: "history entry not found",
                  });
                }
                break;
              }

              case "diagnostics.request": {
                const state = config.registry.getState();
                const files = config.registry.listActiveFiles();
                ws.send(
                  JSON.stringify({
                    action: "diagnostics.response",
                    payload: {
                      epoch: state.epoch,
                      revision: state.revision,
                      activeFiles: files.length,
                    },
                    ...(parsed.requestId !== undefined && {
                      requestId: parsed.requestId,
                    }),
                  }),
                );
                break;
              }
            }
          },

          close(ws) {
            if (ws.data.docAdapter) {
              ws.data.docAdapter._emitClose();
              return;
            }
            wsClients.delete(ws);
            log("info", "WebSocket client disconnected");
          },
        },
      });

      log("info", "Server started", { port: server.port });
    },

    async stop() {
      for (const ws of wsClients) {
        ws.close();
      }
      wsClients.clear();
      server?.stop();
      server = null;
      log("info", "Server stopped");
    },
  };
}

/** Construct a metadata.commit payload from FileMetadata for content-update broadcasts. */
function buildContentUpdateCommit(
  file: {
    fileId: string;
    path: string;
    kind: string;
    deleted: boolean;
    contentAnchor: number;
    contentDigest: string | null;
    contentSize: number | null;
  },
  revision: number,
  epoch: string,
): MetadataCommitResponse {
  return {
    action: "metadata.commit",
    payload: {
      operationId: crypto.randomUUID(),
      fileId: file.fileId,
      path: file.path,
      kind: file.kind,
      deleted: file.deleted,
      contentAnchor: file.contentAnchor,
      revision,
      epoch,
      operationType: "content-update",
      contentDigest: file.contentDigest ?? undefined,
      contentSize: file.contentSize ?? undefined,
    },
  };
}

/** Handle blob upload/download requests. */
async function handleBlobRequest(
  req: Request,
  url: URL,
  config: SyncServerConfig,
  broadcast: (msg: ControlResponse) => void,
): Promise<Response> {
  // /blobs/check/:digest — content-addressed existence check
  if (url.pathname.startsWith("/blobs/check/")) {
    const digest = decodeURIComponent(url.pathname.split("/")[3] ?? "");
    if (!digest) return new Response(null, { status: 400 });
    const exists = config.blobStore.existsByDigest(digest);
    return new Response(null, { status: exists ? 200 : 404 });
  }

  const pathParts = url.pathname.split("/");
  const fileId = decodeURIComponent(pathParts[2] ?? "");

  if (!fileId) {
    return new Response("Bad Request: missing file ID", { status: 400 });
  }

  if (req.method === "PUT") {
    // Validate fileId exists in registry
    const fileMeta = config.registry.getFile(fileId);
    if (!fileMeta) {
      return new Response("Not Found", { status: 404 });
    }

    const contentLength = Number(req.headers.get("content-length") ?? 0);
    if (contentLength > PAYLOAD_LIMITS.content) {
      return new Response("Payload Too Large", { status: 413 });
    }

    const digest = req.headers.get("x-content-digest");
    if (!digest) {
      return new Response("Bad Request: missing X-Content-Digest header", {
        status: 400,
      });
    }

    const body = await req.arrayBuffer();
    const content = new Uint8Array(body);

    // Verify digest matches
    const hasher = new Bun.CryptoHasher("sha256");
    hasher.update(content);
    const computedDigest = hasher.digest("hex");
    if (computedDigest !== digest) {
      return new Response("Bad Request: digest mismatch", { status: 400 });
    }

    const contentAnchor = fileMeta.contentAnchor + 1;

    const record = await config.blobStore.store(
      fileId,
      content,
      digest,
      contentAnchor,
    );

    // Update registry content metadata
    const updatedFile = config.registry.updateContentMetadata(
      fileId,
      digest,
      content.byteLength,
      "blob-upload",
    );

    // Broadcast content-update to all subscribed clients
    if (updatedFile) {
      const state = config.registry.getState();
      broadcast(
        buildContentUpdateCommit(updatedFile, state.revision, state.epoch),
      );
    }

    return new Response(JSON.stringify(record), {
      headers: { "Content-Type": "application/json" },
    });
  }

  if (req.method === "GET") {
    const result = await config.blobStore.retrieve(fileId);
    if (!result) {
      return new Response("Not Found", { status: 404 });
    }
    return new Response(result.content, {
      headers: {
        "Content-Type": "application/octet-stream",
        "X-Content-Digest": result.metadata.digest,
        "Content-Length": String(result.metadata.size),
      },
    });
  }

  if (req.method === "HEAD") {
    const meta = config.blobStore.getMetadata(fileId);
    if (!meta) return new Response(null, { status: 404 });
    return new Response(null, {
      status: 200,
      headers: {
        "X-Content-Digest": meta.digest,
        "Content-Length": String(meta.size),
      },
    });
  }

  return new Response("Method Not Allowed", { status: 405 });
}

/** Handle settings snapshot requests. */
async function handleSettingsRequest(
  req: Request,
  url: URL,
  config: SyncServerConfig,
  broadcast: (msg: ControlResponse) => void,
): Promise<Response> {
  // /settings/:configPath — configPath is relative (e.g. "app.json")
  const configRelativePath = decodeURIComponent(
    url.pathname.slice("/settings/".length),
  );
  // Registry uses canonical vault paths with the config directory prefix
  const configPath = configRelativePath
    ? `.obsidian/${configRelativePath}`
    : "";
  if (!configRelativePath) {
    if (req.method === "GET") {
      // List all tracked settings fileIds
      const fileIds = config.settingsStore.listFileIds();
      return new Response(JSON.stringify({ fileIds }), {
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("Bad Request", { status: 400 });
  }

  // Resolve config_path → fileId via registry
  function resolveFileId(): string | null {
    const existing = config.registry.getFileByPath(configPath);
    return existing?.fileId ?? null;
  }

  function resolveOrCreateFileId(): string {
    const existing = config.registry.getFileByPath(configPath);
    if (existing) return existing.fileId;

    // Create a file identity for this config path
    const result = config.registry.processIntent({
      type: "create",
      clientId: "settings-upload",
      operationId: crypto.randomUUID(),
      path: configPath,
      kind: "text",
    });
    if ("fileId" in result) return result.fileId;
    throw new Error(`Failed to create file identity: ${result.reason}`);
  }

  if (req.method === "PUT") {
    const contentLength = Number(req.headers.get("content-length") ?? 0);
    if (contentLength > PAYLOAD_LIMITS.content) {
      return new Response("Payload Too Large", { status: 413 });
    }

    const digest = req.headers.get("x-content-digest");
    if (!digest) {
      return new Response("Bad Request: missing X-Content-Digest", {
        status: 400,
      });
    }

    const body = await req.arrayBuffer();
    const content = new Uint8Array(body);

    // Verify digest
    const hasher = new Bun.CryptoHasher("sha256");
    hasher.update(content);
    const computedDigest = hasher.digest("hex");
    if (computedDigest !== digest) {
      return new Response("Bad Request: digest mismatch", { status: 400 });
    }

    // Resolve or create file identity, advance anchor through registry
    const fileId = resolveOrCreateFileId();
    const updated = config.registry.updateContentMetadata(
      fileId,
      digest,
      content.byteLength,
      "settings-upload",
    );
    if (!updated) {
      return new Response("Failed to update content metadata", { status: 500 });
    }
    const contentAnchor = updated.contentAnchor;

    const snapshot = config.settingsStore.store(
      fileId,
      content,
      digest,
      contentAnchor,
    );

    // Broadcast content-update to all subscribed clients
    if (updated) {
      const state = config.registry.getState();
      broadcast(buildContentUpdateCommit(updated, state.revision, state.epoch));
    }

    return new Response(JSON.stringify(snapshot), {
      headers: { "Content-Type": "application/json" },
    });
  }

  if (req.method === "GET") {
    const fileId = resolveFileId();
    if (!fileId) {
      return new Response("Not Found", { status: 404 });
    }
    const result = config.settingsStore.getLatest(fileId);
    if (!result) {
      return new Response("Not Found", { status: 404 });
    }
    return new Response(result.content, {
      headers: {
        "Content-Type": "application/octet-stream",
        "X-Content-Digest": result.metadata.digest,
        "X-Content-Anchor": String(result.metadata.contentAnchor),
        "Content-Length": String(result.metadata.size),
      },
    });
  }

  return new Response("Method Not Allowed", { status: 405 });
}

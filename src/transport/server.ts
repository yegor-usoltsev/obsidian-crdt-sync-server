/**
 * Main sync server: HTTP health endpoint, WebSocket control channel,
 * Hocuspocus text doc channels, and blob HTTP endpoints.
 */

import type { Database } from "bun:sqlite";
import type { Server as BunServer, ServerWebSocket } from "bun";
import type { BlobStore } from "../blob-store/blob-store";
import type { HistoryStore } from "../history/history-store";
import type { MetadataRegistry } from "../metadata-registry/registry";
import type { SettingsStore } from "../settings-store/settings-store";
import { log } from "../shared/log";
import { isRateLimited, recordAuthFailure, verifyToken } from "./auth";
import { type ControlResponse, parseControlMessage } from "./messages";

/** Payload size limits. */
export const PAYLOAD_LIMITS = {
  metadata: 256 * 1024, // 256 KiB
  content: 200 * 1024 * 1024, // 200 MiB
} as const;

export interface SyncServerConfig {
  port: number;
  authToken: string;
  dataDir: string;
  db: Database;
  registry?: MetadataRegistry;
  historyStore?: HistoryStore;
  blobStore?: BlobStore;
  settingsStore?: SettingsStore;
}

export interface SyncServer {
  start(): Promise<void>;
  stop(): Promise<void>;
  readonly port: number;
}

interface WsData {
  authenticated: boolean;
  clientId?: string;
  subscribedRevision?: number;
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

            const upgraded = srv.upgrade(req, {
              data: { authenticated: true },
            });
            if (upgraded) return undefined as unknown as Response;
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
            return handleBlobRequest(req, url, config);
          }

          // Settings endpoints
          if (url.pathname.startsWith("/settings/")) {
            return handleSettingsRequest(req, url, config);
          }

          return new Response("Not Found", { status: 404 });
        },

        websocket: {
          open(ws) {
            wsClients.add(ws);
            log("info", "WebSocket client connected");
          },

          message(ws, message) {
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
                if (!config.registry) {
                  send(ws, {
                    action: "error",
                    message: "registry not available",
                  });
                  return;
                }

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

                  // Always send commit to the requesting client
                  send(ws, {
                    action: "metadata.commit",
                    payload: result,
                  });

                  // Broadcast new commits to other clients (not replays)
                  if (!isReplay) {
                    broadcast(
                      {
                        action: "metadata.commit",
                        payload: result,
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
                if (config.historyStore && parsed.sinceRevision !== undefined) {
                  const entries = config.historyStore.getHistorySince(
                    parsed.sinceRevision,
                  );
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
                      },
                    });
                  }
                }
                break;
              }

              case "history.list": {
                if (!config.historyStore) {
                  send(ws, {
                    action: "error",
                    message: "history not available",
                  });
                  return;
                }
                const entries = config.historyStore.getFileHistory(
                  parsed.fileId,
                );
                ws.send(
                  JSON.stringify({
                    action: "history.list",
                    payload: entries,
                  }),
                );
                break;
              }

              case "history.restore": {
                if (!config.historyStore) {
                  send(ws, {
                    action: "error",
                    message: "history not available",
                  });
                  return;
                }
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
                    }),
                  );
                } else {
                  send(ws, {
                    action: "error",
                    message: "history entry not found",
                  });
                }
                break;
              }

              case "diagnostics.request": {
                if (!config.registry) {
                  send(ws, {
                    action: "error",
                    message: "registry not available",
                  });
                  return;
                }
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
                  }),
                );
                break;
              }
            }
          },

          close(ws) {
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

/** Handle blob upload/download requests. */
async function handleBlobRequest(
  req: Request,
  url: URL,
  config: SyncServerConfig,
): Promise<Response> {
  if (!config.blobStore) {
    return new Response("Blob store not available", { status: 503 });
  }

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

    // Get current content anchor from registry
    const fileMeta = config.registry?.getFile(fileId);
    const contentAnchor = (fileMeta?.contentAnchor ?? 0) + 1;

    const record = await config.blobStore.store(
      fileId,
      content,
      digest,
      contentAnchor,
    );

    // Update registry content metadata
    if (config.registry) {
      config.registry.updateContentMetadata(
        fileId,
        digest,
        content.byteLength,
        "blob-upload",
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
): Promise<Response> {
  if (!config.settingsStore) {
    return new Response("Settings store not available", { status: 503 });
  }

  // /settings/:configPath
  const configPath = decodeURIComponent(
    url.pathname.slice("/settings/".length),
  );
  if (!configPath) {
    if (req.method === "GET") {
      // List all tracked settings paths
      const paths = config.settingsStore.listPaths();
      return new Response(JSON.stringify({ paths }), {
        headers: { "Content-Type": "application/json" },
      });
    }
    return new Response("Bad Request", { status: 400 });
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

    const contentAnchor = Number(req.headers.get("x-content-anchor") ?? "1");
    const snapshot = config.settingsStore.store(
      configPath,
      content,
      digest,
      contentAnchor,
    );

    return new Response(JSON.stringify(snapshot), {
      headers: { "Content-Type": "application/json" },
    });
  }

  if (req.method === "GET") {
    const result = config.settingsStore.getLatest(configPath);
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

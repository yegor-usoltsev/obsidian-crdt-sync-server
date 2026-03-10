import { timingSafeEqual } from "node:crypto";
import {
  type afterLoadDocumentPayload,
  type beforeHandleMessagePayload,
  type Document,
  type Extension,
  type Hocuspocus,
  IncomingMessage,
  MessageType,
  type onAuthenticatePayload,
  type onStatelessPayload,
} from "@hocuspocus/server";
import { messageYjsSyncStep2, messageYjsUpdate } from "y-protocols/sync";
import * as Y from "yjs";
import { log } from "../log.ts";
import {
  buildMetadataCommitMessage,
  buildMetadataRejectMessage,
  buildMetadataRejectMessageFromRaw,
  type MetadataOpRequest,
  normalizeMetadataOpRequest,
} from "../messages.ts";
import {
  type AppliedMetadataOp,
  applyMetadataOp,
  CONTENT_DOCUMENT_NAME,
  getCurrentMetadataForFileId,
  initializeMetadataDoc,
  META_DOCUMENT_NAME,
  METADATA_INIT_ORIGIN,
  METADATA_OP_ORIGIN,
} from "../meta-doc.ts";
import { isIgnoredSyncPath } from "../sync-ignore.ts";

export interface MetadataExtensionConfiguration {
  authToken: string;
  maxStatelessPayloadBytes: number;
}

const SERVER_CONNECTION_CONFIG = {
  isAuthenticated: true,
  readOnly: false,
} as const;
const textEncoder = new TextEncoder();

function constantTimeEqual(a: string, b: string): boolean {
  const aBytes = textEncoder.encode(a);
  const bBytes = textEncoder.encode(b);
  if (aBytes.length !== bBytes.length) {
    timingSafeEqual(aBytes, aBytes); // burn equal time before returning
    return false;
  }
  return timingSafeEqual(aBytes, bBytes);
}

function describeDirectMetadataEdit(
  payload: beforeHandleMessagePayload,
): Record<string, unknown> | null {
  const message = new IncomingMessage(payload.update);
  const targetDocumentName = message.readVarString();
  if (targetDocumentName !== META_DOCUMENT_NAME) {
    return null;
  }

  const messageType = message.readVarUint();
  if (
    messageType !== MessageType.Sync &&
    messageType !== MessageType.SyncReply
  ) {
    return null;
  }

  const syncMessageType = message.readVarUint();
  if (syncMessageType === messageYjsUpdate) {
    return {
      messageType,
      syncMessageType,
    };
  }

  if (syncMessageType !== messageYjsSyncStep2) {
    return null;
  }

  const clientUpdate = message.readVarUint8Array();
  const alreadyApplied = Y.snapshotContainsUpdate(
    Y.snapshot(payload.document),
    clientUpdate,
  );

  return alreadyApplied
    ? null
    : {
        messageType,
        syncMessageType,
      };
}

function parseStatelessPayload(payload: string): {
  value?: unknown;
  error?: string;
} {
  try {
    return { value: JSON.parse(payload) };
  } catch (error) {
    return {
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function getIgnoredMetadataPath(request: MetadataOpRequest): string | null {
  switch (request.type) {
    case "file.delete":
      return null;
    case "file.create":
      return isIgnoredSyncPath(
        request.path,
        request.kind === "directory" ? "directory" : "file",
      )
        ? request.path
        : null;
    case "file.rename":
      return isIgnoredSyncPath(
        request.newPath,
        request.kind === "directory" ? "directory" : "file",
      )
        ? request.newPath
        : null;
  }
}

function joinBinaryChunks(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length < 2) {
    return new Uint8Array(chunks[0] ?? []);
  }

  const merged = new Uint8Array(
    chunks.reduce((sum, chunk) => sum + chunk.length, 0),
  );
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }
  return merged;
}

async function fingerprintBytes(bytes: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes));
  return Array.from(new Uint8Array(digest), (byte) => {
    return byte.toString(16).padStart(2, "0");
  }).join("");
}

async function getDeleteContentFingerprint(
  document: Document,
  fileId: string,
): Promise<string | undefined> {
  const files = document.getMap<Y.Text | Uint8Array | Y.Array<Uint8Array>>(
    "files",
  );
  const content = files.get(fileId);
  if (content instanceof Y.Text) {
    return await fingerprintBytes(textEncoder.encode(content.toString()));
  }
  if (content instanceof Uint8Array) {
    return await fingerprintBytes(content);
  }
  if (content instanceof Y.Array) {
    return await fingerprintBytes(joinBinaryChunks(content.toArray()));
  }
  return undefined;
}

const AUTH_RATE_LIMIT_WINDOW_MS = 60_000;
const AUTH_RATE_LIMIT_MAX_FAILURES = 5;

interface RateLimitEntry {
  failures: number;
  windowStart: number;
}

export class MetadataExtension implements Extension {
  readonly extensionName = "metadata";
  private instance: Hocuspocus | null = null;
  private readonly authFailures = new Map<string, RateLimitEntry>();

  constructor(private readonly configuration: MetadataExtensionConfiguration) {}

  private getRemoteAddress(payload: onAuthenticatePayload): string {
    return (
      payload.request?.headers?.["x-forwarded-for"]
        ?.toString()
        ?.split(",")[0]
        ?.trim() ??
      payload.request?.socket?.remoteAddress ??
      "unknown"
    );
  }

  private checkRateLimit(remoteAddress: string): boolean {
    const now = Date.now();
    const entry = this.authFailures.get(remoteAddress);
    if (!entry || now - entry.windowStart > AUTH_RATE_LIMIT_WINDOW_MS) {
      return true;
    }
    return entry.failures < AUTH_RATE_LIMIT_MAX_FAILURES;
  }

  private recordAuthFailure(remoteAddress: string): void {
    const now = Date.now();
    const entry = this.authFailures.get(remoteAddress);
    if (!entry || now - entry.windowStart > AUTH_RATE_LIMIT_WINDOW_MS) {
      this.authFailures.set(remoteAddress, { failures: 1, windowStart: now });
    } else {
      entry.failures += 1;
    }

    // Evict stale entries periodically
    if (this.authFailures.size > 1000) {
      for (const [addr, e] of this.authFailures) {
        if (now - e.windowStart > AUTH_RATE_LIMIT_WINDOW_MS) {
          this.authFailures.delete(addr);
        }
      }
    }
  }

  async onAuthenticate(payload: onAuthenticatePayload): Promise<void> {
    this.instance = payload.instance;
    const remoteAddress = this.getRemoteAddress(payload);

    if (!this.checkRateLimit(remoteAddress)) {
      log("warn", "Authentication rate-limited", {
        documentName: payload.documentName,
        remoteAddress,
      });
      throw new Error("Too many authentication failures");
    }

    if (
      typeof payload.token !== "string" ||
      !constantTimeEqual(payload.token, this.configuration.authToken)
    ) {
      this.recordAuthFailure(remoteAddress);
      log("warn", "Authentication failed", {
        documentName: payload.documentName,
        tokenProvided:
          typeof payload.token === "string" && payload.token.length > 0,
        remoteAddress,
      });
      throw new Error("Unauthorized");
    }

    log("info", "Authentication succeeded", {
      documentName: payload.documentName,
      remoteAddress,
    });

    if (
      payload.documentName !== CONTENT_DOCUMENT_NAME &&
      payload.documentName !== META_DOCUMENT_NAME
    ) {
      log("warn", "Rejected connection for unknown document", {
        documentName: payload.documentName,
      });
      throw new Error("Unknown document");
    }

    if (payload.documentName === META_DOCUMENT_NAME) {
      payload.connectionConfig.readOnly = true;
    }
  }

  async afterLoadDocument(payload: afterLoadDocumentPayload): Promise<void> {
    this.instance = payload.instance;
    if (payload.documentName !== META_DOCUMENT_NAME) {
      return;
    }

    payload.document.transact(() => {
      initializeMetadataDoc(payload.document);
    }, METADATA_INIT_ORIGIN);
  }

  async beforeHandleMessage(
    payload: beforeHandleMessagePayload,
  ): Promise<void> {
    if (
      payload.documentName !== META_DOCUMENT_NAME ||
      payload.connection.readOnly !== true
    ) {
      return;
    }

    const directEdit = describeDirectMetadataEdit(payload);
    if (!directEdit) {
      return;
    }

    log("warn", "Direct client metadata edit blocked", {
      documentName: payload.documentName,
      socketId: payload.socketId,
      ...directEdit,
    });

    throw new Error("Direct metadata edits are not permitted");
  }

  private sendReject(
    payload: onStatelessPayload,
    value: unknown,
    reason: string,
  ): void {
    const rejectMessage = buildMetadataRejectMessageFromRaw(value, reason);
    if (!rejectMessage) {
      return;
    }

    payload.connection.sendStateless(JSON.stringify(rejectMessage));
  }

  async onStateless(payload: onStatelessPayload): Promise<void> {
    const payloadSize = textEncoder.encode(payload.payload).byteLength;
    const parsedPayload = parseStatelessPayload(payload.payload);
    if (payloadSize > this.configuration.maxStatelessPayloadBytes) {
      log("warn", "Metadata stateless payload exceeds size limit", {
        documentName: payload.documentName,
        size: payloadSize,
        limit: this.configuration.maxStatelessPayloadBytes,
      });
      this.sendReject(
        payload,
        parsedPayload.value,
        "Payload exceeds maximum allowed size",
      );
      return;
    }

    if (payload.documentName !== META_DOCUMENT_NAME) {
      return;
    }

    const instance = this.instance;
    if (!instance) {
      throw new Error("Metadata extension is missing server instance");
    }

    let request: MetadataOpRequest;
    try {
      if (parsedPayload.error) {
        throw new Error(parsedPayload.error);
      }

      request = normalizeMetadataOpRequest(parsedPayload.value);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      log("info", "Metadata op rejected (parse error)", { reason });
      this.sendReject(payload, parsedPayload.value, reason);
      return;
    }

    const ignoredPath = getIgnoredMetadataPath(request);
    if (ignoredPath) {
      log("info", "Metadata op rejected (ignored path)", {
        type: request.type,
        fileId: request.fileId,
        clientId: request.clientId,
        path: ignoredPath,
      });
      this.sendReject(payload, request, `Path "${ignoredPath}" is ignored`);
      return;
    }

    let deleteContentFingerprint: string | undefined;
    let contentDocument: Document | null = null;

    if (request.type === "file.delete") {
      contentDocument = await instance.createDocument(
        CONTENT_DOCUMENT_NAME,
        {},
        "server",
        SERVER_CONNECTION_CONFIG,
      );
      deleteContentFingerprint = await getDeleteContentFingerprint(
        contentDocument,
        request.fileId,
      );
    }

    let committed: AppliedMetadataOp | null = null;
    try {
      payload.document.transact(() => {
        initializeMetadataDoc(payload.document);
        committed = applyMetadataOp(
          payload.document,
          request,
          deleteContentFingerprint,
        );
      }, METADATA_OP_ORIGIN);
    } catch (error) {
      if (contentDocument && contentDocument.getConnectionsCount() === 0) {
        await instance.unloadDocument(contentDocument);
      }

      const reason = error instanceof Error ? error.message : String(error);
      log("info", "Metadata op rejected", {
        type: request.type,
        fileId: request.fileId,
        clientId: request.clientId,
        clientOpId: request.clientOpId,
        path: request.type === "file.rename" ? request.newPath : request.path,
        reason,
      });

      payload.connection.sendStateless(
        JSON.stringify(
          buildMetadataRejectMessage(
            request,
            getCurrentMetadataForFileId(payload.document, request.fileId),
            error,
          ),
        ),
      );

      return;
    }

    if (contentDocument && contentDocument.getConnectionsCount() === 0) {
      await instance.unloadDocument(contentDocument);
    }
    if (!committed) {
      throw new Error("Metadata op finished without a commit result");
    }

    const applied = committed as AppliedMetadataOp;

    log("info", "Metadata op committed", {
      type: request.type,
      fileId: request.fileId,
      clientId: request.clientId,
      clientOpId: request.clientOpId,
      path: applied.currentPath,
      oldPath: applied.oldPath,
      newPath: applied.newPath,
      deduplicated: applied.deduplicated,
    });

    if (!applied.deduplicated && applied.deletedContentFileIds?.length) {
      try {
        const contentConnection = await instance.openDirectConnection(
          CONTENT_DOCUMENT_NAME,
        );
        try {
          await contentConnection.transact((document) => {
            const files = document.getMap("files");
            for (const fileId of applied.deletedContentFileIds ?? []) {
              files.delete(fileId);
            }
          });
        } finally {
          await contentConnection.disconnect();
        }
      } catch (error) {
        log("error", "Failed to prune deleted content", {
          documentName: CONTENT_DOCUMENT_NAME,
          fileIds: applied.deletedContentFileIds,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    const commitMessage = JSON.stringify(
      buildMetadataCommitMessage(request, applied),
    );

    if (applied.deduplicated) {
      payload.connection.sendStateless(commitMessage);
    } else {
      payload.document.broadcastStateless(commitMessage);
    }
  }
}

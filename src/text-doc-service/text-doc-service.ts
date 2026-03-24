/**
 * Text document replication service: Yjs/Hocuspocus integration
 * for text-only document sync, keyed by file identity.
 *
 * Uses Hocuspocus (not Server) directly — no listen(), connections
 * are routed via handleConnection() from the Bun HTTP server.
 */

import type { Database } from "bun:sqlite";
import { Hocuspocus } from "@hocuspocus/server";
import * as Y from "yjs";
import type { MetadataRegistry } from "../metadata-registry/registry";
import { log } from "../shared/log";
import { verifyToken } from "../transport/auth";
import type { ControlResponse } from "../transport/messages";

export interface TextDocServiceConfig {
  db: Database;
  authToken: string;
  registry: MetadataRegistry;
  broadcast: (msg: ControlResponse) => void;
}

/**
 * Create the text document Hocuspocus service.
 * Each text file gets its own document name = fileId.
 */
export function createTextDocService(config: TextDocServiceConfig): Hocuspocus {
  function getTrackedTextFile(fileId: string) {
    const file = config.registry.getFile(fileId);
    if (!file || file.deleted || file.kind !== "text") {
      throw new Error("Unknown text document");
    }
    return file;
  }

  const hocuspocus = new Hocuspocus({
    async onAuthenticate(data: { token: string }) {
      const token = data.token;
      if (!token || !verifyToken(token, config.authToken)) {
        throw new Error("Unauthorized");
      }
    },

    async onLoadDocument(data: { documentName: string; document: Y.Doc }) {
      const fileId = data.documentName;
      getTrackedTextFile(fileId);
      log("debug", "Loading text document", { fileId });

      const row = config.db
        .query("SELECT data FROM text_documents WHERE file_id = ?")
        .get(fileId) as { data: Buffer } | null;

      if (row?.data) {
        const update = new Uint8Array(row.data);
        Y.applyUpdate(data.document, update);
      }
    },

    async onStoreDocument(data: { documentName: string; document: Y.Doc }) {
      const fileId = data.documentName;
      getTrackedTextFile(fileId);
      const state = Y.encodeStateAsUpdate(data.document);

      config.db.run(
        `INSERT OR REPLACE INTO text_documents (file_id, data, updated_at)
				 VALUES (?, ?, ?)`,
        [fileId, Buffer.from(state), Date.now()],
      );

      // Extract text and compute digest for content metadata
      const textContent = data.document.getText("content").toString();
      const textBytes = new TextEncoder().encode(textContent);
      const hasher = new Bun.CryptoHasher("sha256");
      hasher.update(textBytes);
      const digest = hasher.digest("hex");

      const updated = config.registry.updateContentMetadata(
        fileId,
        digest,
        textBytes.byteLength,
        "text-doc-store",
      );

      if (updated) {
        // Save text snapshot for restore capability
        config.db.run(
          `INSERT OR REPLACE INTO text_snapshots (file_id, content_anchor, content, stored_at)
           VALUES (?, ?, ?, ?)`,
          [fileId, updated.contentAnchor, textContent, Date.now()],
        );

        // Broadcast content-update to all subscribed clients
        const registryState = config.registry.getState();
        config.broadcast({
          action: "metadata.commit",
          payload: {
            operationId: crypto.randomUUID(),
            fileId: updated.fileId,
            path: updated.path,
            kind: updated.kind,
            deleted: updated.deleted,
            contentAnchor: updated.contentAnchor,
            revision: registryState.revision,
            epoch: registryState.epoch,
            operationType: "content-update",
            contentDigest: updated.contentDigest ?? undefined,
            contentSize: updated.contentSize ?? undefined,
          },
        });
      }
    },
  });

  return hocuspocus;
}

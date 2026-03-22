/**
 * Text document replication service: Yjs/Hocuspocus integration
 * for text-only document sync, keyed by file identity.
 */

import type { Database } from "bun:sqlite";
import { Server as HocuspocusServer } from "@hocuspocus/server";
import * as Y from "yjs";
import { log } from "../shared/log";
import { verifyToken } from "../transport/auth";

export interface TextDocServiceConfig {
  db: Database;
  authToken: string;
}

/**
 * Create the text document Hocuspocus service.
 * Each text file gets its own document name = fileId.
 */
export function createTextDocService(
  config: TextDocServiceConfig,
): HocuspocusServer {
  const hocuspocus = new HocuspocusServer({
    async onAuthenticate(data: { token: string }) {
      const token = data.token;
      if (!token || !verifyToken(token, config.authToken)) {
        throw new Error("Unauthorized");
      }
    },

    async onLoadDocument(data: { documentName: string; document: Y.Doc }) {
      const fileId = data.documentName;
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
      const state = Y.encodeStateAsUpdate(data.document);

      config.db.run(
        `INSERT OR REPLACE INTO text_documents (file_id, data, updated_at)
				 VALUES (?, ?, ?)`,
        [fileId, Buffer.from(state), Date.now()],
      );
    },
  });

  return hocuspocus;
}

/**
 * Ensure text_documents table exists in the database.
 */
export function ensureTextDocTable(db: Database): void {
  db.run(`
		CREATE TABLE IF NOT EXISTS text_documents (
			file_id TEXT PRIMARY KEY,
			data BLOB NOT NULL,
			updated_at INTEGER NOT NULL
		)
	`);
}

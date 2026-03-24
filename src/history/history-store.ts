/**
 * Append-only history storage, restore semantics, and compaction contracts.
 */

import type { Database } from "bun:sqlite";
import * as Y from "yjs";
import type { BlobStore } from "../blob-store/blob-store";
import type { SettingsStore } from "../settings-store/settings-store";
import { getEpoch, nextRevision } from "../shared/database";
import { log } from "../shared/log";
import type { FileId, FileKind, HistoryEntry } from "../shared/types";

export interface HistoryStore {
  /** Get history entries for a file. */
  getFileHistory(fileId: FileId): HistoryEntry[];
  /** Get all history entries since a revision. */
  getHistorySince(revision: number): HistoryEntry[];
  /** Restore a file to a specific history entry, creating a new head. */
  restore(
    fileId: FileId,
    historyEntryId: number,
    clientId: string,
  ): HistoryEntry | null;
  /** Signal compaction: bump epoch so clients rebootstrap safely. */
  compactBefore(revision: number): { deletedEntries: number; newEpoch: string };
  /** Get total history entry count. */
  getEntryCount(): number;
}

export function createHistoryStore(
  db: Database,
  blobStore?: BlobStore,
  settingsStore?: SettingsStore,
): HistoryStore {
  return {
    getFileHistory(fileId: FileId): HistoryEntry[] {
      const rows = db
        .query("SELECT * FROM history WHERE file_id = ? ORDER BY revision ASC")
        .all(fileId) as RawHistoryRow[];
      return rows.map(rowToHistoryEntry);
    },

    getHistorySince(revision: number): HistoryEntry[] {
      const rows = db
        .query("SELECT * FROM history WHERE revision > ? ORDER BY revision ASC")
        .all(revision) as RawHistoryRow[];
      return rows.map(rowToHistoryEntry);
    },

    restore(
      fileId: FileId,
      historyEntryId: number,
      clientId: string,
    ): HistoryEntry | null {
      const row = db
        .query("SELECT * FROM history WHERE id = ? AND file_id = ?")
        .get(historyEntryId, fileId) as RawHistoryRow | null;

      if (!row) return null;

      const revision = nextRevision(db);
      const epoch = getEpoch(db);
      const operationId = crypto.randomUUID();
      const now = Date.now();

      // Create a new canonical head from the restored state (not in-place mutation)
      db.run(
        `UPDATE files SET
					path = ?, kind = ?, deleted = 0, updated_at = ?,
					content_digest = ?, content_size = ?,
					content_anchor = content_anchor + 1
				 WHERE file_id = ?`,
        [row.path, row.kind, now, row.content_digest, row.content_size, fileId],
      );

      // Get the new content_anchor after increment
      const updatedFile = db
        .query("SELECT content_anchor FROM files WHERE file_id = ?")
        .get(fileId) as { content_anchor: number };
      const newAnchor = updatedFile.content_anchor;

      // Restore content based on file kind
      const kind = row.kind as FileKind;
      if (kind === "binary" && blobStore && row.content_anchor > 0) {
        // Copy historical blob to new anchor
        const historical = db
          .query(
            "SELECT digest FROM blobs WHERE file_id = ? AND content_anchor = ?",
          )
          .get(fileId, row.content_anchor) as { digest: string } | null;
        if (historical) {
          // Re-use the same digest at the new anchor
          db.run(
            `INSERT OR REPLACE INTO blobs (file_id, content_anchor, digest, size, stored_at)
             VALUES (?, ?, ?, ?, ?)`,
            [fileId, newAnchor, historical.digest, row.content_size, now],
          );
        }
      } else if (kind === "text" && row.content_anchor > 0) {
        // Restore text from text_snapshots
        const snapshot = db
          .query(
            "SELECT content FROM text_snapshots WHERE file_id = ? AND content_anchor = ?",
          )
          .get(fileId, row.content_anchor) as { content: string } | null;
        if (snapshot) {
          // Create a fresh Y.Doc with the historical text
          const doc = new Y.Doc();
          doc.getText("content").insert(0, snapshot.content);
          const update = Y.encodeStateAsUpdate(doc);
          doc.destroy();
          // Overwrite the text_documents row
          db.run(
            "INSERT OR REPLACE INTO text_documents (file_id, data, updated_at) VALUES (?, ?, ?)",
            [fileId, Buffer.from(update), now],
          );
          // Also store a text snapshot at the new anchor
          db.run(
            "INSERT OR REPLACE INTO text_snapshots (file_id, content_anchor, content, stored_at) VALUES (?, ?, ?, ?)",
            [fileId, newAnchor, snapshot.content, now],
          );
        }
      }
      // Settings restore: settingsStore handles its own per-anchor storage
      if (settingsStore && row.content_anchor > 0) {
        const historical = settingsStore.getByAnchor(
          fileId,
          row.content_anchor,
        );
        if (historical) {
          settingsStore.store(
            fileId,
            historical.content,
            historical.metadata.digest,
            newAnchor,
          );
        }
      }

      // Record the restore in history (append, never mutate)
      db.run(
        `INSERT INTO history (file_id, operation_type, path, kind, content_digest, content_size,
				  content_anchor, client_id, operation_id, timestamp, revision, epoch)
				 VALUES (?, 'restore', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          fileId,
          row.path,
          row.kind,
          row.content_digest,
          row.content_size,
          newAnchor,
          clientId,
          operationId,
          now,
          revision,
          epoch,
        ],
      );

      const insertedId = (
        db.query("SELECT last_insert_rowid() as id").get() as { id: number }
      ).id;

      return {
        id: insertedId,
        fileId,
        operationType: "restore",
        path: row.path,
        kind,
        contentDigest: row.content_digest,
        contentSize: row.content_size,
        contentAnchor: newAnchor,
        clientId,
        operationId,
        timestamp: now,
        revision,
        epoch,
      };
    },

    compactBefore(revision: number): {
      deletedEntries: number;
      newEpoch: string;
    } {
      // Count entries to be deleted
      const countRow = db
        .query("SELECT COUNT(*) as cnt FROM history WHERE revision <= ?")
        .get(revision) as { cnt: number };

      if (countRow.cnt === 0) {
        return { deletedEntries: 0, newEpoch: getEpoch(db) };
      }

      // Delete old history entries
      db.run("DELETE FROM history WHERE revision <= ?", [revision]);

      // Bump epoch — clients with cursors referencing compacted revisions
      // must rebootstrap safely
      const newEpoch = crypto.randomUUID();
      db.run("UPDATE server_state SET value = ? WHERE key = 'epoch'", [
        newEpoch,
      ]);

      // Run incremental vacuum to reclaim space
      db.run("PRAGMA incremental_vacuum");

      log("info", "History compacted", {
        beforeRevision: revision,
        deletedEntries: countRow.cnt,
        newEpoch,
      });

      return { deletedEntries: countRow.cnt, newEpoch };
    },

    getEntryCount(): number {
      const row = db.query("SELECT COUNT(*) as cnt FROM history").get() as {
        cnt: number;
      };
      return row.cnt;
    },
  };
}

interface RawHistoryRow {
  id: number;
  file_id: string;
  operation_type: string;
  path: string;
  kind: string;
  content_digest: string | null;
  content_size: number | null;
  content_anchor: number;
  client_id: string;
  operation_id: string;
  timestamp: number;
  revision: number;
  epoch: string;
}

function rowToHistoryEntry(row: RawHistoryRow): HistoryEntry {
  return {
    id: row.id,
    fileId: row.file_id,
    operationType: row.operation_type as HistoryEntry["operationType"],
    path: row.path,
    kind: row.kind as FileKind,
    contentDigest: row.content_digest,
    contentSize: row.content_size,
    contentAnchor: row.content_anchor,
    clientId: row.client_id,
    operationId: row.operation_id,
    timestamp: row.timestamp,
    revision: row.revision,
    epoch: row.epoch,
  };
}
